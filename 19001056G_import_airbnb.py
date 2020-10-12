import json
import sqlite3      

#Do NOT put functions/statement outside functions

def start():
    #import JSON into DB
    import sqlite3 
    import json 
    conn = sqlite3.connect("airbnb.db") 

    c = conn.cursor() 

    c.execute("DROP TABLE IF EXISTS reviewer") 
    c.execute(''' CREATE TABLE reviewer (rid INTEGER PRIMARY KEY, rname TEXT)''') 

    c.execute("DROP TABLE IF EXISTS accommodation") 
    c.execute(''' CREATE TABLE accommodation (id INTEGER PRIMARY KEY, name TEXT, summary TEXT, url TEXT, review_score_value INTEGER)''') 

    c.execute("DROP TABLE IF EXISTS review") 
    c.execute(''' CREATE TABLE review (id INTEGER PRIMARY KEY AUTOINCREMENT, rid INTEGER , comment TEXT, datetime TEXT, accommodation_id INTEGER,  FOREIGN KEY (rid) REFERENCES reviewer (rid), FOREIGN KEY (accommodation_id) REFERENCES accommodation (id) ) ''') 

    c.execute("DROP TABLE IF EXISTS amenities") 
    c.execute(''' CREATE TABLE amenities (accommodation_id INTEGER, type TEXT, PRIMARY KEY (accommodation_id, type), FOREIGN KEY (accommodation_id) REFERENCES accommodation (id) ) ''') 

    c.execute("DROP TABLE IF EXISTS host") 
    c.execute(''' CREATE TABLE host (host_id INTEGER PRIMARY KEY, host_url TEXT , host_name TEXT, host_about TEXT, host_location TEXT ) ''') 

    c.execute("DROP TABLE IF EXISTS host_accommodation") 
    c.execute(''' CREATE TABLE host_accommodation (host_id INTEGER , accommodation_id INTEGER , PRIMARY KEY (host_id, accommodation_id), FOREIGN KEY (host_id) REFERENCES host (host_id), FOREIGN KEY (accommodation_id) REFERENCES accommodation (id) ) ''') 

    # read file 
    with open('airbnb.json', 'r',encoding="utf8") as myfile:
        data=myfile.read() 
        # parse file 
        listing = json.loads(data)
        
        for i in listing: #[first:last] where last  is not included
        
            intHost_id = i["host"]["host_id"]
            strHost_url = i["host"]["host_url"]
            strHost_name = i["host"]["host_name"]
            strHost_about = i["host"]["host_about"]
            strHost_location = i["host"]["host_location"]

            intAccommodation_id = i["_id"]
            strAccommodationName = i["name"]
            strSummary = i["summary"]
            strUrl = i["listing_url"]

            if "review_scores_value" in i["review_scores"]:
                intReview_score_value = i["review_scores"]["review_scores_value"]
            else:
                intReview_score_value = None


            c.execute("INSERT INTO accommodation (id, name, summary, url, review_score_value ) VALUES (?, ?, ?, ? ,?)", (intAccommodation_id, strAccommodationName, strSummary, strUrl, intReview_score_value))

            try:
                c.execute("INSERT INTO host (host_id, host_url, host_name, host_about, host_location) VALUES (?,?,?,?,?)", (intHost_id, strHost_url, strHost_name, strHost_about, strHost_location))
            except:
                None

            c.execute("INSERT INTO host_accommodation (host_id, accommodation_id) VALUES (?, ?)", (intHost_id, intAccommodation_id))


            amenities = i["amenities"]
            for a in amenities:
                strAmenitiesType = a
                try:
                    c.execute("INSERT INTO amenities (accommodation_id, type ) VALUES (?, ?)", (intAccommodation_id, strAmenitiesType))
                except:
                    None

            reviews = i["reviews"]
            for r in reviews:
                intReviewerId = r["reviewer_id"]
                strReviewerName = r["reviewer_name"]
                strReviewDatetime = r["date"]["$date"]
                strComment = r["comments"]

                c.execute("INSERT INTO review (rid, comment, datetime, accommodation_id ) VALUES (?, ?, ?, ?)", (intReviewerId, strComment, strReviewDatetime, intAccommodation_id))

                try:
                    c.execute("INSERT INTO reviewer (rid, rname) VALUES (?, ?)", (intReviewerId, strReviewerName))
                except:
                    None


        conn.commit()

            

    conn.close()








if __name__ == '__main__':
    start()

