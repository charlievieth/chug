(?# patterns for detecting other log types)

(?# golang log LstdFlags and others - so run more precies regexes first)
^\d{4}\/\d{2}\/\d{2}\ \d{2}\:\d{2}\:\d{2}\ [[:print:]]+

(?# ngninx error log:)
^\d{4}\/\d{2}\/\d{2}\ \d{2}\:\d{2}\:\d{2}\ \[\w+\]\ \d+#\d\:

(?# ngninx access log:)
^([-_.a-zA-Z0-9]*)\ \-\ \[\d{2}\/[A-Z][a-z]{2}\/\d{4}\:\d{2}\:\d{2}\:\d{2}[+ 0-9]*\]
