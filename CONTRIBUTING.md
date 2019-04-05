# Contributing to Brooklin

First of all, thank you for taking the time to contribute to Brooklin.

Code and documentation contributions can be made by forking the repository and sending a pull request against the master branch of this repo.

Please, take some time to review the [Contribution Guidelines](#contribution-guidelines) below before creating a new issue or sending out a PR.

## Contribution Agreement
As a contributor, you represent that the code you submit is your original work or that of your employer (in which case you represent you have the right to bind your employer). By submitting code, you (and, if applicable, your employer) are licensing the submitted code to LinkedIn and the open source community subject to the BSD 2-Clause license.

## Responsible Disclosure of Security Vulnerabilities
Please, do not file reports on Github for security issues.
Please, review the guidelines at https://www.linkedin.com/help/linkedin/answer/62924.
Reports should be encrypted using LinkedIn's [Public PGP Key](https://www.linkedin.com/help/linkedin/answer/79676) and sent to [security@linkedin.com](mailto:security@linkedin.com), preferably with the title "GitHub linkedin/Brooklin — < short summary >".

## Contribution Guidelines

### Java Coding Style Guide
Our Java coding style guidelines are encoded in the [Checkstyle configuration file](https://github.com/linkedin/Brooklin/blob/master/checkstyle/checkstyle.xml) checked into this repo.

### Git Commit Messages Style Guide

1. Separate subject from body with a blank line
2. Limit the subject line to 120 characters
3. Wrap the body at 100 characters
4. Capitalize the subject line
5. Do not end the subject line with a period
6. Do not make references to internal Jira tickets

Here is an example of a good Git commit message:
> ```
> Summarize changes in up to 120 characters
>
> More detailed explanatory text, if necessary. Wrap it to about 100 characters or so. The blank line
> separating the summary from the body is critical (unless you omit the body entirely); tools like
> rebase can get confused if you run the two together. Further paragraphs come after blank lines.
>
>  - Bullet points are okay, too
>  - Typically a hyphen or asterisk is used for the bullet, preceded by a single space, with blank 
>    lines in between, but conventions vary here
> ```

### General Recommendations
- Use Apache Commons for argument validation.
- Make sure to include unit tests whether you are proposing a bug fix, a feature enhancement, or a brand new feature.
