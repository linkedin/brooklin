#!/usr/bin/env python


# Utility for creating well-formed pull request merges and pushing them to Datastream. This script is a modified version
# of the one created by the Spark project (https://github.com/apache/spark/blob/master/dev/merge_spark_pr.py).
#
# Usage: ./merge-pr.py (see config env vars below)
#

import json
import os
import re
import subprocess
import sys
import urllib2

PROJECT_NAME = "Datastream"

CAPITALIZED_PROJECT_NAME = "Datastream".upper()

# Location of the local git repository
REPO_HOME = os.environ.get("%s_HOME" % CAPITALIZED_PROJECT_NAME, os.getcwd())
# Remote name which points to the GitHub site
REMOTE_NAME = "origin"

GITHUB_USER = os.environ.get("GITHUB_USER", "linkedin")
GITHUB_BASE = "https://github.com/%s/%s/pull" % (GITHUB_USER, PROJECT_NAME)
GITHUB_API_BASE = "https://api.github.com/repos/%s/%s" % (GITHUB_USER, PROJECT_NAME)

# Prefix added to temporary branches
TEMP_BRANCH_PREFIX = "PR_TOOL"

def get_json(url):
    try:
        return json.load(urllib2.urlopen(url+"?access_token=dbd086979af74c7dc6461453e03145cfbb918b29"))
    except urllib2.HTTPError as e:
        print "Unable to fetch URL, exiting: %s" % url
        sys.exit(-1)


def fail(msg):
    print msg
    clean_up()
    sys.exit(-1)


def run_cmd(cmd):
    print cmd
    if isinstance(cmd, list):
        return subprocess.check_output(cmd)
    else:
        return subprocess.check_output(cmd.split(" "))

def continue_maybe(prompt):
    result = raw_input("\n%s (y/n): " % prompt)
    if result.lower() != "y":
        fail("Okay, exiting")

def clean_up():
    if original_head != get_current_branch():
        print "Restoring head pointer to %s" % original_head
        run_cmd("git checkout %s" % original_head)

    branches = run_cmd("git branch").replace(" ", "").split("\n")

    for branch in filter(lambda x: x.startswith(TEMP_BRANCH_PREFIX), branches):
        print "Deleting local branch %s" % branch
        run_cmd("git branch -D %s" % branch)

def get_current_branch():
    return run_cmd("git rev-parse --abbrev-ref HEAD").replace("\n", "")

# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref, title, body, pr_repo_desc):
    pr_branch_name = "%s_MERGE_PR_%s" % (TEMP_BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (TEMP_BRANCH_PREFIX, pr_num, target_ref.upper())
    run_cmd("git fetch %s pull/%s/head:%s" % (REMOTE_NAME, pr_num, pr_branch_name))
    run_cmd("git fetch %s %s:%s" % (REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        run_cmd(['git', 'merge', pr_branch_name, '--squash'])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    commit_authors = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                             '--pretty=format:%an <%ae>']).split("\n")
    distinct_authors = sorted(set(commit_authors),
                              key=lambda x: commit_authors.count(x), reverse=True)
    primary_author = raw_input(
        "Enter primary author in the format of \"name <email>\" [%s]: " %
        distinct_authors[0])
    if primary_author == "":
        primary_author = distinct_authors[0]

    reviewers = raw_input(
        "Enter reviewers in the format of \"name1 <email1>, name2 <email2>\": ").strip()

    commits = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                      '--pretty=format:%h [%an] %s']).split("\n")
    
    if len(commits) > 1:
        result = raw_input("List pull request commits in squashed commit message? (y/n): ")
        if result.lower() == "y":
          should_list_commits = True
        else:
          should_list_commits = False
    else:
        should_list_commits = False

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    if body is not None:
        # We remove @ symbols from the body to avoid triggering e-mails
        # to people every time someone creates a public fork of the project.
        merge_message_flags += ["-m", body.replace("@", "")]

    authors = "\n".join(["Author: %s" % a for a in distinct_authors])

    merge_message_flags += ["-m", authors]

    if (reviewers != ""):
        merge_message_flags += ["-m", "Reviewers: %s" % reviewers]

    if had_conflicts:
        committer_name = run_cmd("git config --get user.name").strip()
        committer_email = run_cmd("git config --get user.email").strip()
        message = "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>" % (
            committer_name, committer_email)
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    close_line = "Closes #%s from %s" % (pr_num, pr_repo_desc)
    if should_list_commits:
        close_line += " and squashes the following commits:"
    merge_message_flags += ["-m", close_line]

    if should_list_commits:
        merge_message_flags += ["-m", "\n".join(commits)]

    run_cmd(['git', 'commit', '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe("Merge complete (local ref %s). Push to %s?" % (
        target_branch_name, REMOTE_NAME))

    try:
        run_cmd('git push %s %s:%s' % (REMOTE_NAME, target_branch_name, target_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash

def main():
    global original_head

    original_head = get_current_branch()

    # branches = get_json("%s/branches" % GITHUB_API_BASE)
    # branch_names = filter(lambda x: x.startswith(RELEASE_BRANCH_PREFIX), [x['name'] for x in branches])
    # Assumes branch names can be sorted lexicographically
    # latest_branch = sorted(branch_names, reverse=True)[0]

    pr_num = raw_input("Which pull request would you like to merge? (e.g. 34): ")
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))

    url = pr["url"]

    pr_title = pr["title"]
    commit_title = raw_input("Commit title [%s]: " % pr_title.encode("utf-8")).decode("utf-8")
    if commit_title == "":
        commit_title = pr_title


    body = pr["body"]
    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)

    if not bool(pr["mergeable"]):
        msg = "Pull request %s is not mergeable in its current form.\n" % pr_num + \
            "Continue? (experts only!)"
        continue_maybe(msg)

    print ("\n=== Pull Request #%s ===" % pr_num)
    print ("PR title\t%s\nCommit title\t%s\nSource\t\t%s\nTarget\t\t%s\nURL\t\t%s" % (
        pr_title, commit_title, pr_repo_desc, target_ref, url))
    continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    merged_refs = [target_ref]

    merge_hash = merge_pr(pr_num, target_ref, commit_title, body, pr_repo_desc)

    # pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
    # while raw_input("\n%s (y/n): " % pick_prompt).lower() == "y":
    #     merged_refs = merged_refs + [cherry_pick(pr_num, merge_hash, latest_branch)]

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if (failure_count):
        exit(-1)

    main()
