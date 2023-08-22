# wikipedia_sql_loader
Rust cli tool to load a wikipedia bz2 xml dump to a mediawiki mysql database

WIP

### What currently works

* Inserting articles and text blobs directly in the database
* Used with [sql schema](https://www.mediawiki.org/wiki/Manual:Database_layout) version 1.40

### What does not work

* Mediawiki has transitioned to retrieving article texts from a backend that is external to the database. This backed provides the text blobs used in displaying articles. As such this project does not currently load the article information in a way that Mediawiki currrently supports in displaying.  
