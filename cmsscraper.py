#! python3
import argparse
import concurrent.futures
import html
import json
import os
import re
import string
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

WEB_SERVER = "https://cms.bits-hyderabad.ac.in/"

VALID_FILENAME_CHARS = "-_.() %s%s" % (string.ascii_letters, string.digits)

# Set this to the course category name to fetch courses from a specific category.
# Set to a falsy to ignore category names.
# An example category is "Semester II 2019-20". There can be multiple cataegories
# for example if one semester does not before another begins and there is a
# reason to maintain courses from both semester. This was the case with Sem II of
# 2019-20 and the Summer term (and possible Sem I 2020-21) due to the Covid-19
# pandemic.
COURSE_CATEGORY_NAME = "Semester I - 2020-21"

COURSE_NAME_REGEX = r"^([\w\d \-'&,]+) ([LTP]\d*)(\Z|\s)(.*)$"

# API Endpoints
API_BASE = WEB_SERVER + "webservice/rest/server.php?"
API_CHECK_TOKEN = API_BASE + "wsfunction=core_webservice_get_site_info&moodlewsrestformat=json&wstoken={0}"
API_ENROLLED_COURSES = API_BASE + "wsfunction=core_enrol_get_users_courses&moodlewsrestformat=json&wstoken={0}" \
                       + "&userid={1}"
API_GET_COURSE_CONTENTS = API_BASE + "wsfunction=core_course_get_contents&moodlewsrestformat=json&wstoken={0}" \
                          + "&courseid={1}"
API_GET_ALL_COURSES = API_BASE + "wsfunction=core_course_get_courses_by_field&moodlewsrestformat=json&wstoken={0}"
API_ENROL_COURSE = API_BASE + "wsfunction=enrol_self_enrol_user&moodlewsrestformat=json&wstoken={0}&courseid={1}"
API_GET_FORUM_DISCUSSIONS = API_BASE + "wsfunction=mod_forum_get_forum_discussions_paginated&moodlewsrestformat=json" \
                            + "&sortby=timemodified&sortdirection=DESC&wstoken={0}&forumid={1}&page={2}&perpage={3}"

# Session based webpages
SITE_DASHBOARD = "my/"
SITE_COURSE = "course/view.php?id={0}"

BASE_DIR = os.path.join(os.getcwd(), COURSE_CATEGORY_NAME if COURSE_CATEGORY_NAME else "CMS")

TOKEN = ""

user_id = 0

download_queue = []


def main():

    global TOKEN
    global user_id
    global BASE_DIR

    # setup CLI args
    parser = argparse.ArgumentParser(prog='cmsscrapy.py')
    parser.add_argument('token', help='Moodle WebServices token')

    parser.add_argument('--destination', help='The destination to download files to')
    parser.add_argument('--session-cookie', help='Session cookie obtained after logging in through a browser')
    parser.add_argument('--unenroll-all', action='store_true', help='Uneroll from all courses. ' +
                        'If --all and/or --handouts is specified, download and then unenroll all')
    parser.add_argument('--handouts', action='store_true', help='Download only handouts')
    parser.add_argument('--all', action='store_true', help='Atuomatically enrol to all courses and download files')
    parser.add_argument('--preserve', action='store_true', help='Preserves the courses you are enrolled to. ' +
                        ' If the --all flag is specified, then you must provide a session cookie to unenroll from ' +
                        ' courses.')

    args = parser.parse_args()
    TOKEN = args.token

    if args.destination is not None:
        BASE_DIR = os.path.join(os.path.abspath(os.path.expanduser(args.destination)),
                                COURSE_CATEGORY_NAME if COURSE_CATEGORY_NAME else "CMS")


    response = requests.request("get", API_CHECK_TOKEN.format(TOKEN))
    if response.status_code == 200:
        js = json.loads(response.text)
        if "exception" in js and js["errorcode"] == "invalidtoken":
            print("Couldn't verify token. Invalid token.")

        user_id = js['userid']
        os.makedirs(BASE_DIR, exist_ok=True)

        if args.session_cookie is None:
            if args.unenroll_all:
                print("Cannot uneroll from courses without providing session cookie")
                sys.exit()

            if args.preserve and args.all:
                print("Cannot uneroll from courses without providing session cookie.")
                sys.exit()

        if args.unenroll_all and args.preserve:
            print("Cannot specify --unenroll-all and --preserve together")
            sys.exit()

        if args.unenroll_all and not args.all and not args.handouts:
            # unenroll all courses and exit out
            unenroll_all(args.session_cookie)
        else:
            if args.preserve:
                enrolled_courses = get_enrolled_courses()

            if args.all:
                enrol_all_courses()

            if args.handouts:
                download_handouts()
            else:
                download_all()

            if args.preserve and args.all:
                unenroll_all(args.session_cookie)
                enroll_courses(enrolled_courses)
    else:
        print("Bad response code while verifying token: " + str(response.status_code))


def enrol_all_courses():
    """Enroll a user to all courses listed on CMS"""
    print("Enrolling to all courses")
    enroll_courses(get_all_courses())


def enroll_courses(courses):
    """Enroll to all specified courses"""
    enrolled_courses = [x['id'] for x in get_enrolled_courses()]
    to_enrol = [x for x in courses if not x["id"] in enrolled_courses]
    with ThreadPoolExecutor(max_workers=25) as executor:
        executor.map(enrol_course, [x["id"] for x in to_enrol], [x["fullname"] for x in to_enrol])
        executor.shutdown(wait=True)


def enrol_course(id, fullname):
    requests.request("get", API_ENROL_COURSE.format(TOKEN, id))
    print("Enrolled in course: " + html.unescape(fullname))


def download_all():
    directories = enquee_all_downloads()
    print("Starting downloads")
    if download_queue:
        start_downloads()
    else:
        print("Nothing to download!")

    # delete all empty diretories
    for d in reversed(directories):
        if os.path.exists(d) and len(os.listdir(d)) == 0:
            os.rmdir(d)


def enquee_all_downloads():
    # pre-compile the regex expression
    # the regex group represents the fully qualified name of the course (excluding the year and sem info)
    regex = re.compile(COURSE_NAME_REGEX)

    # get the list of enrolled courses
    courses = get_enrolled_courses()

    # holds the list of directories created... all empty directories will be deleted as part of cleanup
    directories = []

    print("Enqueueing downloads")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for course in courses:
            match = regex.match(html.unescape(course["fullname"]))
            if not match:
                continue
            futures.append(executor.submit(enquee_course_downloads, course, match[1], match[2]))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            print("Finished processing: " + ", ".join((result[0], result[1], result[4] if len(result) == 5 else "")))
            directories.extend(result[2])

    return directories


def enquee_course_downloads(course, course_name, section_name):
    directories = []

    course_name = removeDisallowedFilenameChars(course_name)
    course_dir = os.path.join(BASE_DIR, course_name, section_name)

    # create folders
    os.makedirs(course_dir, exist_ok=True)
    directories.append(course_dir)

    course_id = course["id"]
    response = requests.request("get", API_GET_COURSE_CONTENTS.format(TOKEN, course_id))
    course_sections = json.loads(response.text)
    for course_section in course_sections:
        # create folder with name of the course_section
        course_section_name = removeDisallowedFilenameChars(course_section["name"])[:50].strip()
        course_section_dir = os.path.join(course_dir, course_section_name)
        os.makedirs(course_section_dir, exist_ok=True)
        directories.append(course_section_dir)

        # Sometimes professors use section descriptions as announcements and embed file links
        summary = course_section["summary"]
        if summary:
            soup = BeautifulSoup(summary, features="lxml")
            anchors = soup.find_all('a')
            if anchors:
                for anchor in anchors:
                    link = anchor.get('href')
                    if link:
                        if WEB_SERVER in link:
                            # file is on the same domain, download it
                            # we don't know the file name, we use w/e is provided by the server
                            submit_download(get_final_download_link(link, TOKEN), course_section_dir, None)

        if "modules" not in course_section:
            continue

        for module in course_section["modules"]:
            # if it's a forum, there will be discussions which each need a folder
            module_name = removeDisallowedFilenameChars(module["name"])[:50].strip()
            module_dir = os.path.join(course_section_dir, module_name)
            os.makedirs(module_dir, exist_ok=True)
            directories.append(module_dir)

            if module["modname"] in ("resource", "folder"):
                for content in module["contents"]:
                    file_url = content["fileurl"]
                    file_url = get_final_download_link(file_url, TOKEN)
                    if module["name"].lower() == "handout":
                        # if the module is for handout, save the file as HANDOUT followed by the file extension
                        file_name = "".join(("HANDOUT", content["filename"][content["filename"].rfind("."):]))
                    else:
                        file_name = removeDisallowedFilenameChars(content["filename"])

                    out_path = os.path.join(module_dir, file_name)
                    if os.path.exists(out_path) and os.path.getsize(out_path) == int(content["filesize"]):
                        continue  # skip if we've already downloaded
                    submit_download(file_url, module_dir, file_name)
            elif module["modname"] == "forum":
                forum_id = module["instance"]
                # (0, 0) -> Returns all discussion
                response = requests.request("get", API_GET_FORUM_DISCUSSIONS.format(TOKEN, forum_id, 0, 0))
                response_json = json.loads(response.text)
                if "exception" in response_json:
                    break   # probably no discussion associated with module

                forum_discussions = json.loads(response.text)["discussions"]
                for forum_discussion in forum_discussions:
                    forum_discussion_name = removeDisallowedFilenameChars(forum_discussion["name"][:50].strip())
                    forum_discussion_dir = os.path.join(module_dir, forum_discussion_name)
                    os.makedirs(forum_discussion_dir, exist_ok=True)
                    directories.append(forum_discussion_dir)

                    if not forum_discussion["attachment"] == "":
                        for attachment in forum_discussion["attachments"]:
                            file_url = attachment["fileurl"]
                            file_url = get_final_download_link(file_url, TOKEN)
                            out_path = os.path.join(forum_discussion_dir,
                                                    removeDisallowedFilenameChars(attachment["filename"]))

                            if os.path.exists(out_path) and os.path.getsize(out_path) == int(attachment["filesize"]):
                                continue  # skip if we've already downloaded
                            submit_download(file_url, forum_discussion_dir,
                                            removeDisallowedFilenameChars(attachment["filename"]))
    return (course_name, section_name, directories)


def download_handouts():
    """Downloads handouts for all courses whose names matches the regex"""

    # pre-compile the regex expression
    # the first regex group represents the course code and the name of the course
    # the second regex group represents only the course code
    regex = re.compile(COURSE_NAME_REGEX)

    print("Downloading handouts")

    # get the list of enrolled courses
    response = requests.request("get", API_ENROLLED_COURSES.format(TOKEN, user_id))

    courses = json.loads(response.text)
    for course in courses:
        full_name = html.unescape(course["fullname"]).strip()
        match = regex.match(full_name)
        if not match:
            continue
        print("Processing:", full_name)
        course_id = course["id"]
        response = requests.request("get", API_GET_COURSE_CONTENTS.format(TOKEN, course_id))
        course_sections = json.loads(response.text)
        for course_section in course_sections:
            for module in course_section["modules"]:
                if module["name"].lower().strip() == "handout":
                    content = module["contents"][0]
                    if content["type"] == "file":
                        file_url = content["fileurl"]
                        file_url = get_final_download_link(file_url, TOKEN)
                        file_ext = content["filename"][content["filename"].rfind("."):]

                        short_name = removeDisallowedFilenameChars(match[1].strip()) + "_HANDOUT"
                        print(short_name)
                        if submit_download(file_url, BASE_DIR, short_name, file_ext=file_ext):
                            break
            else:
                continue
            break
    start_downloads()


def unenroll_all(session_cookie):
    # Get and set the session cookie
    cookies = {'MoodleSession': session_cookie}

    # Check if session is valid
    session = requests.Session()
    session.cookies = requests.cookies.cookiejar_from_dict(cookies)
    r = session.post(WEB_SERVER + SITE_DASHBOARD)
    if r.status_code == 303:
        print("Invalid session cookie. Try again.")
        return

    print("Unenrolling all courses")

    courses = get_enrolled_courses()
    with ThreadPoolExecutor(max_workers=10) as executor:
        for course in courses:
            executor.submit(unerol_course, course, cookies)


def unerol_course(course, cookies):
    session = requests.Session()
    session.cookies = requests.cookies.cookiejar_from_dict(cookies)
    course_id = course["id"]
    r = session.post(WEB_SERVER + SITE_COURSE.format(course_id))
    soup = BeautifulSoup(r.content, features="lxml")
    anchors = soup.find_all("a", href=re.compile("unenrolself.php"))
    if anchors:
        unenrol = anchors[0]["href"]
        r = session.post(unenrol)
        soup = BeautifulSoup(r.content, features="lxml")
        form = soup.find("form", action="https://td.bits-hyderabad.ac.in/moodle/enrol/self/unenrolself.php")
        if form:
            enrolid = form.find("input", {"name": "enrolid"})["value"]
            sesskey = form.find("input", {"name": "sesskey"})["value"]

            payload = {"enrolid": enrolid, "confirm": "1", "sesskey": sesskey}
            r = session.post("https://td.bits-hyderabad.ac.in/moodle/enrol/self/unenrolself.php", data=payload)
            if r.status_code == 200:
                print("Unenrolled from: ", course["fullname"])
            else:
                print("Failed to unenroll from: ", course["fullname"])


def get_all_courses():
    response = requests.request("get", API_GET_ALL_COURSES.format(TOKEN))
    courses = json.loads(response.text)["courses"]
    if COURSE_CATEGORY_NAME:
        courses = [x for x in courses if x["categoryname"] == COURSE_CATEGORY_NAME]
    return courses


def get_enrolled_courses():
    response = requests.request("get", API_ENROLLED_COURSES.format(TOKEN, user_id))
    return json.loads(response.text)


def start_downloads():
    if not download_queue:
        print("Nothing queued for download")
        return

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for item in download_queue:
            futures.append(executor.submit(download_file, *item))

        for future in concurrent.futures.as_completed(futures):
            ret = future.result()
            if ret:
                print("Downloaded file:", ret[1])


def submit_download(file_url, file_dir, file_name, file_ext=""):
    download_queue.append((file_url, file_dir, file_name, file_ext))


def download_file(file_url, file_dir, file_name, file_ext=""):
    """Downloads the file at file_url and saves at the file_name. If file_ext is apened to end of file_name"""
    with requests.get(file_url, stream=True) as response:
        check_exists = False
        if not file_name:
            file_name = response.headers['content-disposition']
            file_name = re.findall("filename=\"(.+)\"", file_name)[0]
            check_exists = True  # since file name was not known when enqueeing, we check if file exists here

        path = os.path.join(file_dir, file_name + file_ext)

        # if int(response.headers['content-length']) > 100 * 1024 * 1024:
        #     # skip files greater than 100 MB and a blacklisted extension
        #     t = [x not in path for x in ('.mp4', '.mov', '.rar')]
        #     if not all(t):
        #         return

        # Ignore if file already exists
        if check_exists and os.path.exists(path) and os.path.getsize(path) == int(response.headers['content-length']):
            return

        print("Downloading file:", file_url,
              "Length=%s" % human_readable_sizeof_fmt(int(response.headers['content-length'])))

        with open(path, "wb+") as f:
            for chunk in response.iter_content(100*1024*1024):
                f.write(chunk)
        return (True, path)

def get_final_download_link(file_url, token):
    token_parameter = "".join(("&token=", TOKEN) if "?" in file_url else ("?token=", TOKEN))
    return "".join((file_url, token_parameter))


def human_readable_sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def removeDisallowedFilenameChars(filename):
    cleanedFilename = unicodedata.normalize('NFKD', filename).encode('ASCII', 'ignore')
    return ''.join(chr(c) for c in cleanedFilename if chr(c) in VALID_FILENAME_CHARS)


if __name__ == "__main__":
    main()
