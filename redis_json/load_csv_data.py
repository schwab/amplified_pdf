import pandas as pd
import csv
key_files = {"version":"csv/bible_version_key.csv", 
         "books_english":"csv/key_english.csv",
         "genre":"csv/key_genre_english.csv",
         "key_abbreviations_english":"csv/key_abbreviations_english.csv"
         }
key_columns = {"version":["id","book_file_prefix", "abbreviation","language","name", "EMPTY", "wiki", "unknown_1","domain", "unknown_2"], 
           "books_english":["id","book_name","testament","genre_id"],
           "genre":["id", "genre"],
           "key_abbreviations_english":["id","abbreviation", "book_id", "NONE"]
           }

def load_version() -> dict:
    version = []
    with open(key_files["version"], newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        row_c = 0
        columns = key_columns["version"]
        for row in reader:
            r_data = {}
            if row_c > 0:
                
                for i in range(len(columns)):
                    r_data[columns[i]] = row[i]
                    
                version.append(r_data)
            row_c += 1
        return version
    
    
    
    
def main():
    if "__main__" in __name__:
        print(load_version())
            
        
main()
        
        