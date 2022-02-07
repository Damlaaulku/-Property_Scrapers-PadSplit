Working logic of the project:

1- "ddl.py" file has to work once. It creates database tables and writes the fields to the spreadsheet

2- After "insert_data()" function works on all scrapers (opendoor,offerpad,flexmls,fmls,georgiamls,harmls,ntreismls), hoa_logic.py has to work.
insert_data() functions and hoa_logic.py has to be frequently.

insert_data(): works with all APIs, inserts data to database and spreatsheet.
hoa_logic.py: applies hoa logic to the spreadsheet table.

Note:You should add the client_services.json with your own google credentials. 
You should share the document with the "client_email" to be able to edit it.