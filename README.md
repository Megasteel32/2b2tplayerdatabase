# 2b2t Player Database
## Overview
Sup. I created this database by scraping [2b2t.dev](2b2t.dev) for their player list and last seen data. All credit to that data goes to them, I simply aggregated the data into an easily searchable database. Currently, there are 457,768 players with 238,858 corresponding last seen dates. I do not currently have any join data, that is not public to my knowledge. This data was scraped across 12/5-12/6/2021, therefore it is already out of date by the time you read this. However, it's here, I'll be playing around with this data set and others to see what I can come up with. Any updates will be noted below, for now this is all I've got.
**TO DOWNLOAD THE DB: [CLICK HERE](https://www.mediafire.com/file/w92le0v5hc2srtl/2b2t.accdb/file)**
Due to Github limitations (and I can't be arsed to use GLFS) the file is too big to easily host here.
## Database Usage
The database is a .accdb file for Microsoft Access. There are tools/ways out there to convert this to a MySQL, SQLite, or otherwise a different database format/management suite. There are two tables, players and last seen. The **player table** holds username, UUID, joins/leaves, kills/deaths. The **last seen table** should be self explanatory, holding username and last seen data. The tables are linked using username as primary keys. 

There is one query and one form as well, you can use either since the form is based off of the query. For those new to Access, just double click either and enter your username (or the username you want to search for) into the text box and hit enter. Using the query shows the results in a table-like format, while the form presents the data a little cleaner, and right clicking the form after searching for data allows you to export to HTML or a variety of other formats for viewing. 

You are more than welcome to host this database elsewhere, I hope to create a much easier way of accessing the data soon (web page w/ simple search + clean results output). 
## Data Notes
This data is accurate from 2018ish onward. I do not know the exact date, but if you're an old player and the data seems off, especially if you haven't been active, that's probably why. If anyone has parts or the whole of this data preceding that date, please contact me: [megasteel32@gmail.com](mailto:Megasteel32@gmail.com)
## Future Data Inclusion
I am currently gathering additional data about different factions on 2b2t, such as specific players in each and hope to create a visual of which faction has the best K/D or cool visualizations like that. Check back here for more updates.  
