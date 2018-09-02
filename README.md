Veil: Deidentify data
======================

Veil is a collection of utilities to help deidentify datasets. Currently, the main purpose of Veil is to provide 
an easy way to create a reference table for an ID column that is sensitive and to map it to any number of downstream
dataframes. Veil relies on `Pandas` to do most of the heavy lifting, and simply provides a way to concisely deidentify
and reidentify columns where necessary. 

TODO:
* Add module to work with date columns
* Provide more saving formats for `id_veil.save()`
* Add tests

License
-------
MIT