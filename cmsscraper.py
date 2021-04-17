#! python3
import argparse
import asyncio
import html
import json
import logging
import logging.config
import os
import re
import string
import unicodedata
from functools import partial

import aiohttp
from bs4 import BeautifulSoup

WEB_SERVER = "https://cms.bits-hyderabad.ac.in"

VALID_FILENAME_CHARS = "-_.() %s%s" % (string.ascii_letters, string.digits)

# An example category is "Semester II 2019-20". There can be multiple cataegories
# for example if one semester does not before another begins and there is a
# reason to maintain courses from both semester. This was the case with Sem II of
# 2019-20 and the Summer term (and possible Sem I 2020-21) due to the Covid-19
# pandemic.
COURSE_CATEGORY_NAME = ""

COURSE_NAME_REGEX = r"^([\w\d \-\/'&,]+) ([LTP]\d*)(\Z|\s)(.*)$"

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

download_queue = []

course_categories = []

session: aiohttp.ClientSession = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100))


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

    logger.info("Verifying Token")
    response = await session.get(API_CHECK_TOKEN.format(TOKEN))
    if not response.status == 200:
        logger.error("Bad response code while verifying token: " + str(response.status))
        return

    js = json.loads(await response.text())
    if 'exception' in js and js['errorcode'] == 'invalidtoken':
        logger.error("Couldn't verify token. Invalid token.")
        return

    user_id = js['userid']
    await async_makedirs(BASE_DIR)

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
        await unenrol_all(args.session_cookie)
    else:
        if args.preserve:
            enrolled_courses = await get_enroled_courses()
            with open('preserved.json', 'w') as f:
                json.dump(enrolled_courses, f)

        if args.restore:
            with open('preserved.json', 'r') as f:
                to_enrol = json.load(f)
                enrol_courses(to_enrol)

        if args.all:
            await enrol_all_courses()

        if args.handouts:
            await download_handouts()
        else:
            await download_enroled_courses()

        # Await all pending downloads that were submitted
        pending = asyncio.Task.all_tasks()
        await asyncio.gather(*pending)

        if args.preserve and args.all:
            await unenrol_all(args.session_cookie)
            await enrol_courses(enrolled_courses)


async def enrol_all_courses():
    """Enroll a user to all courses listed on CMS"""
    logger.info("Enrolling to all courses")
    await enrol_courses(await get_all_courses())


async def enrol_courses(courses: dict):
    """Enrol to all specified courses"""
    enroled_courses = [x['id'] for x in await get_enroled_courses()]
    to_enrol = [x for x in courses if x["id"] not in enroled_courses]
    futures = [enrol_course(x['id'], x['fullname']) for x in to_enrol]
    await asyncio.gather(*futures)


async def enrol_course(id: int, fullname: str):
    logger.info(f'Enroling to course: {html.unescape(fullname)}')
    await session.get(API_ENROL_COURSE.format(TOKEN, id))


async def download_enroled_courses():
    # the regex group represents the fully qualified name of the course (excluding the year and sem info)
    regex = re.compile(COURSE_NAME_REGEX)

    # get the list of enrolled courses
    courses = await get_enroled_courses()

    async def process(course, course_name, section_name):
        logger.info(f'Processing course {course_name} {section_name}')
        course_name = removeDisallowedFilenameChars(course_name)
        course_dir = os.path.join(BASE_DIR, course_name, section_name)

        # create folders
        await async_makedirs(course_dir)

        course_id = course["id"]
        # TODO: Create method to get course contents
        response = await session.get(API_GET_COURSE_CONTENTS.format(TOKEN, course_id))
        course_sections = json.loads(await response.text())

        for x in course_sections:
            asyncio.ensure_future(download_course_section(x, course_dir))

    for course in courses:
        match = regex.match(html.unescape(course["fullname"]))
        if not match:
            continue
        asyncio.ensure_future(process(course, match[1], match[2]))


async def download_course_section(course_section: dict, course_dir: str):
    # create folder with name of the course_section
    course_section_name = removeDisallowedFilenameChars(course_section["name"])[:50].strip()
    course_section_dir = os.path.join(course_dir, course_section_name)
    await async_makedirs(course_section_dir)

    # Sometimes professors use section descriptions as announcements and embed file links
    summary = course_section["summary"]
    if summary:
        soup = BeautifulSoup(summary, features="lxml")
        anchors = soup.find_all('a')
        if not anchors:
            return
        for anchor in anchors:
            link = anchor.get('href')
            # Download the file only if it's on the same domain
            if not link or WEB_SERVER not in link:
                continue
            # we don't know the file name, we use w/e is provided by the server
            download_link = get_final_download_link(link, TOKEN)
            asyncio.ensure_future(download_file(download_link, course_section_dir, ""))

    if "modules" not in course_section:
        return

    for module in course_section["modules"]:
        asyncio.ensure_future(download_module(module, course_section_dir))


async def download_module(module: dict, course_section_dir: str):
    # if it's a forum, there will be discussions each of which need a folder
    module_name = removeDisallowedFilenameChars(module["name"])[:50].strip()
    module_dir = os.path.join(course_section_dir, module_name)
    await async_makedirs(module_dir)

    if module["modname"].lower() in ("resource", "folder"):
        for content in module["contents"]:
            file_url = content["fileurl"]
            file_url = get_final_download_link(file_url, TOKEN)
            if module["name"].lower() == "handout":
                # rename handouts to HANDOUT
                file_name = "".join(("HANDOUT", content["filename"][content["filename"].rfind("."):]))
            else:
                file_name = removeDisallowedFilenameChars(content["filename"])

            asyncio.ensure_future(download_file(file_url, module_dir, file_name))
    elif module["modname"] == "forum":
        forum_id = module["instance"]
        # (0, 0) -> Returns all discussion
        response = await session.get(API_GET_FORUM_DISCUSSIONS.format(TOKEN, forum_id, 0, 0))
        if not response.ok:
            logger.warning(f'Server responded with {response.status} for {response.real_url}... Retrying')
            # Schedule this coroutine to run once again
            asyncio.ensure_future(download_module(module, course_section_dir))
            return
        response_json = json.loads(await response.text())
        if "exception" in response_json:
            return   # probably no discussion associated with module

        forum_discussions = response_json["discussions"]
        for forum_discussion in forum_discussions:
            forum_discussion_name = removeDisallowedFilenameChars(forum_discussion["name"][:50].strip())
            forum_discussion_dir = os.path.join(module_dir, forum_discussion_name)
            await async_makedirs(forum_discussion_dir)

            if not forum_discussion["attachment"] == "":
                for attachment in forum_discussion["attachments"]:
                    file_url = get_final_download_link(attachment["fileurl"], TOKEN)
                    file_name = removeDisallowedFilenameChars(attachment["filename"])
                    asyncio.ensure_future(download_file(file_url, forum_discussion_dir, file_name))


async def download_handouts():
    """Downloads handouts for all courses whose names matches the regex"""
    regex = re.compile(COURSE_NAME_REGEX)

    logger.info("Downloading handouts")

    # get the list of enrolled courses
    response = await session.get(API_ENROLLED_COURSES.format(TOKEN, user_id))
    courses = json.loads(await response.text())

    async def process(course):
        full_name = html.unescape(course["fullname"]).strip()
        match = regex.match(full_name)
        if not match:
            return
        logger.info(f"Processing: {full_name}")
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
                        logger.info("Downloading:", short_name)
                        await download_file(file_url, BASE_DIR, short_name, file_ext=file_ext)
            else:
                continue
            break

    await asyncio.gather(*[process(x) for x in courses])


async def unenrol_all():
    # Check if session is valid
    response: aiohttp.ClientResponse = await session.post(WEB_SERVER + SITE_DASHBOARD)
    if response.status == 303:
        logger.error('Invalid session cookie')
        return

    courses = await get_enroled_courses()
    logger.info(f'Unerolling from {len(courses)} courses')

    futures = [unenrol_course(x) for x in courses]
    asyncio.gather(futures)


async def unenrol_course(course: dict, retry_count: int = 0):
    course_id = course["id"]

    r = await session.post(WEB_SERVER + SITE_COURSE.format(course_id))
    if not r.ok:
        if retry_count >= 10:
            logger.failed(f'Failed to unenrol from {course["fullname"]}, exceeded retries')
            return
        asyncio.ensure_future(unenrol_course(course, retry_count+1))
        return

    soup = BeautifulSoup(await r.text(), features='lxml')
    anchors = soup.find_all('a', href=re.compile('.*unenrolself.php'))
    if not anchors:
        logger.warning(f'Failed to unenroll from: {course["fullname"]}... No anchors found')
        asyncio.ensure_future(unenrol_course(course, retry_count+1))
        return

    unenrol_link = anchors[0]['href']
    r = session.post(unenrol_link)
    if not r.ok:
        if retry_count >= 10:
            logger.error(f'Failed to unenrol from {course["fullname"]}, exceeded retries')
            return
        asyncio.ensure_future(unenrol_course(course, retry_count+1))
        return

    soup = BeautifulSoup(r.content, features="lxml")
    form = soup.find('form', action=f'{WEB_SERVER}/enrol/self/unenrolself.php')
    if not form:
        logger.error(f'Failed to unenroll from: {course["fullname"]}... Form not found')
        asyncio.ensure_future(unenrol_course(course, retry_count+1))
        return

    enrolid = form.find('input', {'name': 'enrolid'})['value']
    sesskey = form.find('input', {'name': 'sesskey'})['value']

    payload = {'enrolid': enrolid, 'confirm': '1', 'sesskey': sesskey}
    r = session.post(f'{WEB_SERVER}/enrol/self/unenrolself.php', data=payload)
    if r.status_code >= 200 and r.status_code < 400:
        logger.info('Unenrolled from: ', course['fullname'])
    else:
        logger.error(f'Failed to unenroll from: {course["fullname"]}... Final post failed')
        asyncio.ensure_future(unenrol_course(course, retry_count+1))


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


async def download_file(file_url: str, file_dir: str, file_name: str, file_ext: str = "") -> bool:
    """Download file asynchronously

    if `file_name` is empty, file name will be obtained from the
    `Content-Disposition` header of the response. If this is empty as well,
    the file is not downloaded.

    If the download fails for whatever reason, it is requeed.
    """
    try:
        async with session.get(file_url, chunked=1024) as response:
            response: aiohttp.ClientResponse
            if not response.ok:
                logger.warning(f'Server responded with {response.status} when downloading {response.real_url}... Skipping')
                return

            if not file_name:
                if not response.content_disposition:
                    logger.error(f'Cannot download {file_url}... Empty file name and content disposititon')
                    return False
                file_name = response.content_disposition.filename

            path = os.path.join(file_dir, file_name + file_ext)

            # Ignore if file already exists
            length = int(response.headers['content-length'])
            humanized_length = humanized_sizeof(length)
            if os.path.exists(path) and os.path.getsize(path) == length:
                return False

            if length >= 512 * 1024 * 1024:
                logger.info(f'Skipping file: {file_url}, Length={humanized_length}, exceeds 500MiB')
                return False

            logger.info(f'Downloading file: {file_url}, Length={humanized_length}')

            with open(path, "wb+") as f:
                async for chunk in response.content.iter_chunked(1024):
                    f.write(chunk)
            return True
    except asyncio.TimeoutError:
        asyncio.ensure_future(download_file(file_url, file_dir, file_name, file_ext))


async def async_makedirs(path, *args, **kwargs):
    loop = asyncio.get_event_loop()
    pfunc = partial(os.makedirs, path, *args, **kwargs, exist_ok=True)
    return await loop.run_in_executor(None, pfunc)


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
