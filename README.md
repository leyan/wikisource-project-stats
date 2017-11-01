# Goal

It is currently very hard to know what is going on in the English Wikisource. The Recent Changes mechanism is not adapted because it works page by page and provides no information for a whole book.

The idea is to parse the Wikisource data to extract project-level information: degree of completion, recent changes, number of contributors, etc.

# Process

A separate database is created. This is initialized from the most recent dump of the main en.wikisource database, then kept up-to-date from the Recent Changes API.



