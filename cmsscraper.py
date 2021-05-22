#! python3
import argparse
import asyncio
import html
import logging
import logging.config
import os
import re
import string
import unicodedata
import queue
from functools import partial
from typing import List

import aiohttp
import ujson
from bs4 import BeautifulSoup

json = ujson  # TODO: Replace json references with ujson instead of setting global variable

WEB_SERVER = "https://cms.bits-hyderabad.ac.in"

VALID_FILENAME_CHARS = "-_.() %s%s" % (string.ascii_letters, string.digits)

# An example category is "Semester II 2019-20". There can be multiple cataegories
# for example if one semester does not before another begins and there is a
# reason to maintain courses from both semester. This was the case with Sem II of
# 2019-20 and the Summer term (and possible Sem I 2020-21) due to the Covid-19
# pandemic.
COURSE_CATEGORY_NAME = ""

COURSE_NAME_REGEX = r"^([\w\d \-\/'&,]+) ([LTP]\d*)(\Z|\s)(.*)$"

SEMAPHORE_COUNT = 25

# API Endpoints
API_BASE = WEB_SERVER + "/webservice/rest/server.php?"
API_CHECK_TOKEN = API_BASE + "wsfunction=core_webservice_get_site_info&moodlewsrestformat=json&wstoken={0}"
API_ENROLLED_COURSES = API_BASE + "wsfunction=core_enrol_get_users_courses&moodlewsrestformat=json&wstoken={0}" \
                       + "&userid={1}"
API_GET_COURSE_CONTENTS = API_BASE + "wsfunction=core_course_get_contents&moodlewsrestformat=json&wstoken={0}" \
                          + "&courseid={1}"
API_GET_ALL_COURSES = API_BASE + "wsfunction=core_course_get_courses_by_field&moodlewsrestformat=json&wstoken={0}"
API_ENROL_COURSE = API_BASE + "wsfunction=enrol_self_enrol_user&moodlewsrestformat=json&wstoken={0}&courseid={1}"
API_GET_FORUM_DISCUSSIONS = API_BASE + "wsfunction=mod_forum_get_forum_discussions_paginated&moodlewsrestformat=json" \
                            + "&sortby=timemodified&sortdirection=DESC&wstoken={0}&forumid={1}&page={2}&perpage={3}"
API_GET_COURSE_CATEGORIES = API_BASE + "wsfunction=core_course_get_categories&moodlewsrestformat=json&wstoken={0}"

# Session based webpages
SITE_DASHBOARD = "/my/"
SITE_COURSE = "/course/view.php?id={0}"

BASE_DIR = os.path.join(os.getcwd(), COURSE_CATEGORY_NAME if COURSE_CATEGORY_NAME else "CMS")

TOKEN = ""

logger: logging.Logger = logging.getLogger()

user_id = 0

download_queue = queue.Queue()  # A thread safe queue

course_categories = []

session: aiohttp.ClientSession = aiohttp.ClientSession(connector=aiohttp.TCPConnector(),
                                                       timeout=aiohttp.ClientTimeout(total=100))


async def main():

    global TOKEN
    global user_id
    global BASE_DIR
    global COURSE_CATEGORY_NAME
    global course_categories
    global session

    # setup CLI args
    parser = argparse.ArgumentParser(prog='cmsscrapy.py')
    parser.add_argument('token', help='Moodle WebServices token')
    parser.add_argument(
        '--category',
        type=str,
        help='The name of the category of which courses are downloaded from.',
        default=''
    )
    parser.add_argument('--destination', help='The destination to download files to')
    parser.add_argument('--session-cookie', help='Session cookie obtained after logging in through a browser')
    parser.add_argument('--unenroll-all', action='store_true', help='Uneroll from all courses. ' +
                        'If --all and/or --handouts is specified, download and then unenroll all')
    parser.add_argument('--handouts', action='store_true', help='Download only handouts')
    parser.add_argument('--all', action='store_true', help='Atuomatically enrol to all courses and download files')
    parser.add_argument('--preserve', action='store_true', help='Preserves the courses you are enrolled to. ' +
                        ' If the --all flag is specified, then you must provide a session cookie to unenroll from ' +
                        ' courses.')
    parser.add_argument('--restore', action='store_true', help='Restores previously preserved courses. To be used after'
                        ' unenrolling from all courses.')

    args = parser.parse_args()

    TOKEN = args.token
    COURSE_CATEGORY_NAME = args.category

    if args.destination is not None:
        BASE_DIR = os.path.join(os.path.abspath(os.path.expanduser(args.destination)),
                                COURSE_CATEGORY_NAME if COURSE_CATEGORY_NAME else "CMS")

    response = await session.get(API_CHECK_TOKEN.format(TOKEN))
    if not response.status == 200:
        logger.error("Bad response code while verifying token: " + str(response.status))
        return
    logger.info("Token Verified")

    js = json.loads(await response.text())
    if 'exception' in js and js['errorcode'] == 'invalidtoken':
        logger.error("Couldn't verify token. Invalid token.")
        return

    user_id = js['userid']
    async_makedirs(BASE_DIR)

    if args.session_cookie is None:
        if args.unenroll_all:
            logger.error("Cannot uneroll from courses without providing session cookie")
            return

    if args.unenroll_all and args.preserve:
        logger.error("Cannot specify --unenroll-all and --preserve together")
        return

    session.cookie_jar.update_cookies({'MoodleSession': args.session_cookie})

    course_categories = await get_course_categories()

    if args.unenroll_all and not args.all and not args.handouts:
        # unenroll all courses and exit out
        await unenrol_all()
    else:
        if args.preserve:
            enrolled_courses = await get_enroled_courses()
            with open('preserved.json', 'w') as f:
                json.dump(enrolled_courses, f)

        if args.restore:
            with open('preserved.json', 'r') as f:
                to_enrol = json.load(f)
                await enrol_courses(to_enrol)
                logger.info('Restored previously preserved courses!')
                return

        if args.all:
            await enrol_all_courses()

        # Await any queued futures before we continue
        # If this is not done, synchronization issues will arise
        if args.handouts:
            await asyncio.gather(*await queue_handouts())
        else:
            await asyncio.gather(*await queue_enroled_courses())

        if download_queue.qsize() > 0:
            logger.info(f"Downloading {download_queue.qsize()} files...")
            returns = await process_download_queue()
            logging.info(f'Finished processing downloads... Skipped {returns.count(False)} files')
        else:
            logger.info("No files to download!")

        if args.preserve and args.all:
            await unenrol_all()
            await enrol_courses(enrolled_courses)


async def enrol_all_courses():
    """Enroll a user to all courses listed on CMS"""
    logger.info("Enrolling to all courses")
    await enrol_courses(await get_all_courses())


async def enrol_courses(courses: dict):
    """Enrol to all specified courses"""
    sem = asyncio.Semaphore(SEMAPHORE_COUNT)
    enroled_courses = set([x['id'] for x in await get_enroled_courses()])
    to_enrol = [x for x in courses if x["id"] not in enroled_courses]
    futures = [enrol_course(sem, x['id'], x['fullname']) for x in to_enrol]
    await asyncio.gather(*futures)


async def enrol_course(sem: asyncio.Semaphore, id: int, fullname: str):
    logger.info(f'Enroling to course: {html.unescape(fullname)}')
    async with sem:
        await session.get(API_ENROL_COURSE.format(TOKEN, id))


async def queue_enroled_courses() -> List[asyncio.Future]:
    # the regex group represents the fully qualified name of the course (excluding the year and sem info)
    regex = re.compile(COURSE_NAME_REGEX)
    awaitables = []

    # get the list of enrolled courses
    logging.info("Fetching enroled courses")
    courses = await get_enroled_courses()
    sem = asyncio.Semaphore(SEMAPHORE_COUNT)

    async def process(sem, course, course_name, section_name) -> List[asyncio.Future]:
        awaitables = []
        course_name = removeDisallowedFilenameChars(course_name)
        course_dir = os.path.join(BASE_DIR, course_name, section_name)

        # create folders
        awaitables.append(async_makedirs(course_dir))

        course_id = course["id"]
        # TODO: Create method to get course contents
        async with sem:
            response = await session.get(API_GET_COURSE_CONTENTS.format(TOKEN, course_id))
        course_sections = json.loads(await response.text())

        tasks = []
        for x in course_sections:
            tasks.append(queue_course_section(sem, x, course_dir))

        for x in await asyncio.gather(*tasks):
            awaitables += x

        logger.info(f'Finished Processing course {course_name} {section_name}')
        return awaitables
    tasks = []
    for course in courses:
        match = regex.match(html.unescape(course["fullname"]))
        if not match:
            continue
        tasks.append(process(sem, course, match[1], match[2]))

    for x in await asyncio.gather(*tasks):
        awaitables += x
    return awaitables


async def queue_course_section(sem: asyncio.Semaphore, course_section: dict, course_dir: str) -> List[asyncio.Future]:
    # create folder with name of the course_section
    awaitables = []
    course_section_name = removeDisallowedFilenameChars(course_section["name"])[:50].strip()
    course_section_dir = os.path.join(course_dir, course_section_name)
    awaitables.append(async_makedirs(course_section_dir))

    # Sometimes professors use section descriptions as announcements and embed file links
    summary = course_section["summary"]
    if summary:
        soup = BeautifulSoup(summary, features="lxml")
        anchors = soup.find_all('a')
        if not anchors:
            return awaitables
        for anchor in anchors:
            link = anchor.get('href')
            # Download the file only if it's on the same domain
            if not link or WEB_SERVER not in link:
                continue
            # we don't know the file name, we use w/e is provided by the server
            download_link = get_final_download_link(link, TOKEN)
            awaitables.append(add_to_download_queue(download_link, course_section_dir, "", "", -1))

    if "modules" not in course_section:
        return awaitables

    tasks = []
    for module in course_section["modules"]:
        tasks.append(queue_module(sem, module, course_section_dir))

    for x in await asyncio.gather(*tasks):
        awaitables += x
    return awaitables


async def queue_module(sem: asyncio.Semaphore, module: dict, course_section_dir: str) -> List[asyncio.Future]:
    # if it's a forum, there will be discussions each of which need a folder
    awaitables = []
    module_name = removeDisallowedFilenameChars(module["name"])[:50].strip()
    module_dir = os.path.join(course_section_dir, module_name)
    awaitables.append(async_makedirs(module_dir))

    if module["modname"].lower() in ("resource", "folder"):
        for content in module["contents"]:
            file_url = content["fileurl"]
            file_size = content["filesize"]
            file_url = get_final_download_link(file_url, TOKEN)
            if module["name"].lower() == "handout":
                # rename handouts to HANDOUT
                file_name = "".join(("HANDOUT", content["filename"][content["filename"].rfind("."):]))
            else:
                file_name = removeDisallowedFilenameChars(content["filename"])

            awaitables.append(add_to_download_queue(file_url, module_dir, file_name, "", file_size))
    elif module["modname"] == "forum":
        forum_id = module["instance"]
        # (0, 0) -> Returns all discussion
        async with sem:
            response = await session.get(API_GET_FORUM_DISCUSSIONS.format(TOKEN, forum_id, 0, 0))
        if not response.ok:
            logger.warning(f'Server responded with {response.status} for {response.real_url}... Skipping')
            return awaitables
        response_json = json.loads(await response.text())
        if "exception" in response_json:
            return awaitables  # probably no discussion associated with module

        forum_discussions = response_json["discussions"]
        for forum_discussion in forum_discussions:
            forum_discussion_name = removeDisallowedFilenameChars(forum_discussion["name"][:50].strip())
            forum_discussion_dir = os.path.join(module_dir, forum_discussion_name)
            awaitables.append(async_makedirs(forum_discussion_dir))

            if isinstance(forum_discussion["attachment"], list):
                for attachment in forum_discussion["attachments"]:
                    file_url = get_final_download_link(attachment["fileurl"], TOKEN)
                    file_name = removeDisallowedFilenameChars(attachment["filename"])
                    file_size = attachment["filesize"]
                    awaitables.append(add_to_download_queue(file_url, forum_discussion_dir, file_name, "", file_size))
    return awaitables


async def queue_handouts():
    """Downloads handouts for all courses whose names matches the regex"""
    regex = re.compile(COURSE_NAME_REGEX)
    awaitables = []

    logger.info("Downloading handouts")

    # get the list of enrolled courses
    response = await session.get(API_ENROLLED_COURSES.format(TOKEN, user_id))
    courses = json.loads(await response.text())

    async def process(course):
        full_name = html.unescape(course["fullname"]).strip()
        match = regex.match(full_name)
        if not match:
            return

        course_id = course["id"]
        response = await session.get(API_GET_COURSE_CONTENTS.format(TOKEN, course_id))
        course_sections = json.loads(await response.text())
        for course_section in course_sections:
            for module in course_section["modules"]:
                if module["name"].lower().strip() == "handout":
                    content = module["contents"][0]
                    if content["type"] == "file":
                        file_url = content["fileurl"]
                        file_url = get_final_download_link(file_url, TOKEN)
                        file_ext = content["filename"][content["filename"].rfind("."):]
                        short_name = removeDisallowedFilenameChars(match[1].strip()) + "_HANDOUT"

                        logging.info(f'Queuing handout for {full_name}')
                        awaitables.append(add_to_download_queue(file_url, BASE_DIR, short_name, file_ext, -1))
                        break
            else:
                continue
            break

    await asyncio.gather(*[process(x) for x in courses])
    return awaitables


async def unenrol_all():
    # Check if session is valid
    response: aiohttp.ClientResponse = await session.post(WEB_SERVER + SITE_DASHBOARD)
    if response.status == 303:
        logger.error('Invalid session cookie')
        return

    logger.info("Fetching user courses")
    courses = await get_enroled_courses()
    logger.info(f'Unenroling from {len(courses)} courses')

    sem = asyncio.Semaphore(SEMAPHORE_COUNT)
    futures = [unenrol_course(sem, x) for x in courses]
    await asyncio.gather(*futures, return_exceptions=True)


async def unenrol_course(sem: asyncio.Semaphore, course: dict):
    async with sem:
        course_id = course["id"]

        r = await session.post(WEB_SERVER + SITE_COURSE.format(course_id))
        if not r.ok:
            logger.error(f'Failed to unenrol from {course["fullname"]}')

        soup = BeautifulSoup(await r.text(), features='lxml')
        anchors = soup.find_all('a', href=re.compile('.*unenrolself.php'))
        if not anchors:
            logger.warning(f'Failed to unenroll from: {course["fullname"]}... No anchors found')
            return

        unenrol_link = anchors[0]['href']
        r = await session.post(unenrol_link)
        if not r.ok:
            logger.error(f'Failed to unenrol from {course["fullname"]}')
            return

        soup = BeautifulSoup(await r.text(), features="lxml")
        form = soup.find('form', action=f'{WEB_SERVER}/enrol/self/unenrolself.php')
        if not form:
            logger.error(f'Failed to unenroll from: {course["fullname"]}... Form not found')
            return

        enrolid = form.find('input', {'name': 'enrolid'})['value']
        sesskey = form.find('input', {'name': 'sesskey'})['value']

        payload = {'enrolid': enrolid, 'confirm': '1', 'sesskey': sesskey}
        r = await session.post(f'{WEB_SERVER}/enrol/self/unenrolself.php', data=payload)
        if r.ok:
            logger.info(f'Unenrolled from: {course["fullname"]}')
        else:
            logger.error(f'Failed to unenroll from: {course["fullname"]}... Final post failed')


async def get_all_courses() -> dict:
    response = await session.get(API_GET_ALL_COURSES.format(TOKEN))
    courses = json.loads(await response.text())["courses"]
    if COURSE_CATEGORY_NAME:
        courses = [x for x in courses if x["categoryname"] == COURSE_CATEGORY_NAME]
    return courses


async def get_enroled_courses() -> dict:
    response = await session.get(API_ENROLLED_COURSES.format(TOKEN, user_id))
    courses = json.loads(await response.text())
    if COURSE_CATEGORY_NAME:
        category_id = get_category_id_from_name(COURSE_CATEGORY_NAME)
        courses = [x for x in courses if x["category"] and x["category"] == category_id]
    return courses


async def get_course_categories() -> dict:
    response = await session.get(API_GET_COURSE_CATEGORIES.format(TOKEN))
    return json.loads(await response.text())


def add_to_download_queue(file_url: str, file_dir: str, file_name: str, file_ext: str,
                          file_size: int = -1) -> asyncio.Future:
    def process(file_url: str, file_dir: str, file_name: str, file_ext: str, file_size: int):
        # Check if file already exists and only then add it to the queue
        path = os.path.join(file_dir, file_name + file_ext)
        if not file_size == -1 and os.path.exists(path) and os.stat(path).st_size == file_size:
            return

        if file_size >= 512 * 1024 * 1024:
            logger.info(f'Skipping file: {file_url}, Length={humanized_sizeof(file_size)}, exceeds 500MiB')
            return

        download_queue.put((file_url, file_dir, file_name, file_ext))

    loop = asyncio.get_event_loop()
    pfunc = partial(process, file_url, file_dir, file_name, file_ext, file_size)
    return loop.run_in_executor(None, pfunc)


async def process_download_queue() -> List[bool]:
    tasks = []
    sem = asyncio.Semaphore(SEMAPHORE_COUNT)
    for param in list(download_queue.queue):
        tasks.append(download_file(sem, *param))
    return await asyncio.gather(*tasks)


async def download_file(
    sem: asyncio.Semaphore,
    file_url: str,
    file_dir: str,
    file_name: str,
    file_ext: str = ""
) -> bool:
    """Download file asynchronously

    if `file_name` is empty, file name will be obtained from the
    `Content-Disposition` header of the response. If this is empty as well,
    the file is not downloaded.

    If the download fails for whatever reason, it is requeed.
    """
    try:
        async with sem, session.get(file_url) as response:
            response: aiohttp.ClientResponse
            if not response.ok:
                logger.warning(f'Server responded with {response.status} when downloading'
                               f' {response.real_url} ... Skipping')
            if not file_name:
                if not response.content_disposition:
                    logger.error(f'Cannot download {file_url} ... Empty file name and content disposititon')
                    return False
                file_name = response.content_disposition.filename

            path = os.path.join(file_dir, file_name + file_ext)

            # Ignore if file already exists
            length = int(response.headers['content-length'])
            humanized_length = humanized_sizeof(length)

            logger.info(f'Downloading file: {file_url}, Length={humanized_length}')

            with open(path, "wb+") as f:
                f.write(await response.content.read())
            return True
    except BaseException:
        logger.warning(f'Exception downloading {file_url}... Skipping')
        return False


def async_makedirs(path, *args, **kwargs) -> asyncio.Future:
    """Make directories asynchronously by using the default loop executor"""
    loop = asyncio.get_event_loop()
    pfunc = partial(os.makedirs, path, *args, **kwargs, exist_ok=True)
    return loop.run_in_executor(None, pfunc)


def get_category_id_from_name(category_name: str) -> int:
    for category in course_categories:
        if category["name"] == category_name:
            return category["id"]


def get_final_download_link(file_url, token):
    token_parameter = "".join(("&token=", TOKEN) if "?" in file_url else ("?token=", TOKEN))
    return "".join((file_url, token_parameter))


def humanized_sizeof(num: int, unit: str = 'B') -> str:
    """Convert `num` from base `unit` to human readable base-2 size string

    num: Size in bytes
    suffix: Unit suffix, default 'B' for bytes
    """
    for prefix in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, prefix, unit)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', unit)


def removeDisallowedFilenameChars(filename: str) -> str:
    """Remove disallowed characters from given filename"""
    cleanedFilename = unicodedata.normalize('NFKD', filename).encode('ASCII', 'ignore')
    return ''.join(chr(c) for c in cleanedFilename if chr(c) in VALID_FILENAME_CHARS)


if __name__ == "__main__":
    LOG_CONF = {
        'version': 1,
        'formatters': {
            'simple': {
                'format': '%(asctime)s - %(levelname)s - %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'simple',
                'stream': 'ext://sys.stdout',
            },
        },
        'loggers': {
            '': {
                'level': 'DEBUG',
                'handlers': ['console', ]
            }
        }
    }
    logging.config.dictConfig(LOG_CONF)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_until_complete(session.close())
