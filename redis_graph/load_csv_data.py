key_files = {"version":"csv/bible_version_key.csv", 
         "books_english":"csv/key_english.csv",
         "genre":"csv/key_genre_english.csv",
         "key_abbreviations_english":"csv/key_abbreviations_english.csv"
         }
key_columns = {"version":["id","book_file_prefix", "abbreviation","language","name", "wiki", "unknown_1","domain", "unknown_2"], 
           "books_english":["id","book_name","testament","genre_id"],
           "genre":["id", "genre"],
           "key_abbreviations_english":["id","abbreviation"]
           }

def load_keys():
    for key in key_files:
        