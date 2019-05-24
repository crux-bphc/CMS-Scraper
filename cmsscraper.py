import html
import json
import os
import re

import requests

web_server = "https://td.bits-hyderabad.ac.in/moodle/"
api_base = web_server + "webservice/rest/server.php?"
api_check_token = api_base + "wsfunction=core_webservice_get_site_info&moodlewsrestformat=json&wstoken={0}"
api_course_search = api_base + "wsfunction=core_course_search_courses&moodlewsrestformat=json&criterianame=search&wstoken={0}&criteriavalue={1}&page={2}&perpage={3}"
api_enrolled_courses = api_base + "wsfunction=core_enrol_get_users_courses&moodlewsrestformat=json&wstoken={0}&userid={1}"
api_get_course_contents = api_base + "wsfunction=core_course_get_contents&moodlewsrestformat=json&wstoken={0}&courseid={1}"
api_get_all_courses = api_base + "wsfunction=core_course_get_courses_by_field&moodlewsrestformat=json&wstoken={0}"
api_enrol_course = api_base + "wsfunction=enrol_self_enrol_user&moodlewsrestformat=json&wstoken={0}&courseid={1}"
api_get_forum_discussions = api_base + "wsfunction=mod_forum_get_forum_discussions_paginated&moodlewsrestformat=json&sortby=timemodified&sortdirection=DESC&wstoken={0}&forumid={1}&page={2}&perpage={3}"
base_dir = os.getcwd() + "/CMS/"

token = ""

user_id = 0

re_sanitize = re.compile(r"[<>:\"/\\|?*]|\.\.")


def main():
    global user_id

    if token == "":
        input("Enter token")

    response = requests.request("get", api_check_token.format(token))
    if response.status_code == 200:
        js = json.loads(response.text)
        if "exception" in js and js["errorcode"] == "invalidtoken":
            print("Couldn't verify token. Invalid token.")
        user_id = js['userid']

        os.makedirs(base_dir, exist_ok=True)

        handouts_only = 1 if input("Download only handouts? Y/n: ").lower() == "y" else 0
        enrol_all_courses = 1 if input("Enrol all courses? Y/n: ").lower() == "y" else 0

        if enrol_all_courses: enrol_courses()  # enrol to all courses on CMS
        
        if handouts_only:
            download_handouts()
        else:
            download_all()
    else:
        print("Bad response code while verifying token: " + response.status_code)


def enrol_courses():
    """enrols a user to all courses listed on CMS"""

    response = requests.request("get", api_get_all_courses.format(token))
    courses = json.loads(response.text)["courses"]

    response = requests.request("get", api_enrolled_courses.format(token, user_id))
    courses_enrolled = json.loads(response.text)

    for course in courses:
        already_enrolled = 0
        for course_enrolled in courses_enrolled:
            if course_enrolled["id"] == course["id"]:
                already_enrolled = 1
                break
        if already_enrolled: continue
        print("Enrolling in course: " + html.unescape(course["fullname"]))
        requests.request("get", api_enrol_course.format(token, course["id"]))


def download_all():
    # pre-compile the regex expression
    # the regex group represents the fully qualified name of the course (excluding the year and sem info)
    regex = re.compile(r"([\w\d /\-'&]+) - 2018-19")

    # get the list of enrolled courses
    response = requests.request("get", api_enrolled_courses.format(token, user_id))
    courses = json.loads(response.text)
    for course in courses:
        full_name = html.unescape(course["fullname"])

        match = regex.match(full_name)
        if not match: continue

        print("Processing: " + match[1])
        course_name = sanitize_string(match[1])
        course_dir = base_dir + "/" + course_name

        # create folder with name of course
        os.makedirs(course_dir, exist_ok=True)

        course_id = course["id"]
        response = requests.request("get", api_get_course_contents.format(token, course_id))
        course_sections = json.loads(response.text)
        for course_section in course_sections:
            # create folder with name of the course_section
            course_section_name = sanitize_string(course_section["name"])[:50].strip()  # retain a maximum of 50 characters
            course_section_dir = course_dir + "/" + course_section_name
            os.makedirs(course_section_dir, exist_ok=True)

            if not course_section["summary"] == "":
                with open(course_section_dir + "/desc.html", "w+", encoding="utf-8") as f:
                    f.write(course_section["summary"])

            if "modules" in course_section:
                # if there are no modules, this means the prof wanted the course section name to be the info, save that as the text file
                if len(course_section["modules"]) == 0:
                    with open(course_section_dir + "/read_me.txt", "w+", encoding="utf-8") as f:
                        f.write(course_section["name"])

                for module in course_section["modules"]:
                    # if it's a forum, there will be discussions which each need a folder
                    module_name = sanitize_string(module["name"])[:50].strip()  # retain a maximum of 20 characters
                    module_dir = course_section_dir + "/" + module_name
                    os.makedirs(module_dir, exist_ok=True)

                    with open(module_dir + "/mod_name.txt", "w+", encoding="utf-8") as f:
                        f.write(module["name"])

                    if "description" in module and not module["description"] == "":
                        with open(module_dir + "/desc.html", "w+", encoding="utf-8") as f:
                            f.write(module["description"])

                    if module["modname"] in ("resource", "folder"):
                        for content in module["contents"]:
                            file_url = content["fileurl"]
                            file_url = file_url + "&token=" + token if "?" in file_url else file_url + "?token=" + token  # append the token to the end of the url
                            file_name = sanitize_string(content["filename"].strip()) if not module[
                                                                                                "name"].lower() == "handout" else "HANDOUT" + \
                                                                                                                                  content[
                                                                                                                                      "filename"][
                                                                                                                                  content[
                                                                                                                                      "filename"].rfind(
                                                                                                                                      "."):]
                            out_path = module_dir + "/" + file_name
                            if os.path.exists(out_path) and os.path.getsize(out_path) == content["filesize"]: continue
                            download_file(file_url, module_dir + "/" + file_name)
                    elif module["modname"] == "forum":
                        forum_id = module["instance"]
                        response = requests.request("get", api_get_forum_discussions.format(token, forum_id, 0, 0)) # (0, 0) -> Returns all discussion
                        response_json = json.loads(response.text)
                        if "exception" in response_json: break  # probably no discussion associated with module

                        forum_discussions = json.loads(response.text)["discussions"]
                        for forum_discussion in forum_discussions:
                            forum_discussion_name = sanitize_string(forum_discussion["name"][:50].strip())
                            forum_discussion_dir = module_dir + "/" + forum_discussion_name
                            os.makedirs(forum_discussion_dir, exist_ok=True)

                            if not forum_discussion["message"] == "":
                                with open(forum_discussion_dir + "/desc.html", "w+", encoding="utf-8") as f:
                                    f.write(forum_discussion["message"])

                            if not forum_discussion["attachment"] == "":
                                for attachment in forum_discussion["attachments"]:
                                    file_url = attachment["fileurl"]
                                    file_url = file_url + "&token=" + token if "?" in file_url else file_url + "?token=" + token  # append the token to the end of the url
                                    out_path = forum_discussion_dir + "/" + sanitize_string(attachment["filename"])
                                    if os.path.exists(out_path) and os.path.getsize(out_path) == attachment[
                                        "filesize"]: continue
                                    download_file(file_url, out_path)

                            # delete the directory if there are no files in it
                            if len(next(os.walk(forum_discussion_dir))[2]) == 0:
                                os.rmdir(forum_discussion_dir)

            # delete the directory if there are no files in it
            if len(os.listdir(course_section_dir)) == 0:
                os.rmdir(course_section_dir)


def download_handouts():
    """Downloads handouts for all courses whose names matches the regex"""

    # pre-compile the regex expression
    # the first regex group represents the course code and the name of the course
    # the second regex group represents only the course code
    regex = re.compile(r"(([\w/ ]+ F[\d]{3}) [\w &\-']+) [PL][1 ]")

    # get the list of enrolled courses
    response = requests.request("get", api_enrolled_courses.format(token, user_id))
    courses = json.loads(response.text)
    for course in courses:
        full_name = html.unescape(course["fullname"])
        match = regex.match(full_name)
        if not match: continue
        course_id = course["id"]
        response = requests.request("get", api_get_course_contents.format(token, course_id))
        course_sections = json.loads(response.text)
        for course_section in course_sections:
            for module in course_section["modules"]:
                if module["name"].lower() == "handout":
                    content = module["contents"][0]
                    if content["type"] == "file":
                        file_url = content["fileurl"]
                        file_url = file_url + "&token=" + token if "?" in file_url else file_url + "?token=" + token  # append the token to the end of the url
                        file_ext = content["filename"][content["filename"].rfind("."):]

                        short_name = sanitize_string(match[1].strip())
                        if download_file(file_url, base_dir + short_name + "_HANDOUT", file_ext=file_ext):
                            break
            else:
                continue
            break
        else:
            continue


def download_file(file_url, file_name, file_ext=""):
    """Downloads the file at file_url and saves at the file_name. If file_ext is apened to end of file_name"""

    response = requests.request("get", file_url)
    with open(file_name + file_ext, "wb+") as f:
        f.write(response.content)
        print("Downloaded file " + file_name + file_ext)
        return True

    return False


def sanitize_string(string):
    """Sanitizes a directory or folder name by removing characters illegal in Windows"""
    return re_sanitize.sub("_", string).strip()


if __name__ == "__main__":
    main()
