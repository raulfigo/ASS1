from flask import Flask, request, jsonify 
import json
import sqlite3


app = Flask(__name__)
app.config['DEBUG'] = True


# Show your student ID
@app.route('/mystudentID/', methods=['GET'])
def my_student_id():    
    response={"studentID": "19001056G"}
    return jsonify(response), 200, {'Content-Type': 'application/json'}



@app.route('/airbnb/reviews/', methods=['GET'])
def getReviews():

    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    strSql = ""
    strWhereStart = ""
    strWhereEnd = ""

    if 'start' in request.args.keys():
        start = str(request.args['start'])
        strWhereStart = " AND date(Datetime) >=date('" + start + "')"

    if 'end' in request.args.keys():
        end = str(request.args['end'])
        strWhereEnd = " AND date(Datetime) <= date('" + end + "')"


    strSql = "SELECT * FROM review WHERE 1=1 " + strWhereStart + strWhereEnd + " order by datetime, rid"

    cur.execute(strSql)

    reviews = []

    conn.commit() 

    reviewRows = cur.fetchall()
    reviewCount = len(reviewRows)

    #access the table rows by column name 
    for reviewRow in reviewRows: 
        intReviewID = reviewRow['id']
        intRid = reviewRow['rid']
        strComment = reviewRow['comment']
        strDatetime = reviewRow['Datetime']
        intAccommodation_id = reviewRow['accommodation_id']

        conn2 = sqlite3.connect("airbnb.db") 
        conn2.row_factory = sqlite3.Row
        cur2 = conn2.cursor() 

        cur2.execute("SELECT rname FROM reviewer where rid =" + str(intRid))
        conn2.commit() 
        reviewerRows = cur2.fetchall()
        for reviewerRow in reviewerRows[0:1]: 
            strRname = reviewerRow['rname']
            conn2.close

        reviews.append(
                        {               
                            "Accommodation ID": intAccommodation_id,
                            "Comment": strComment,
                            "DateTime": strDatetime,
                            "Reviewer ID": intRid,
                            "Reviewer Name": strRname
                        }
                      )

    conn.close()
    return jsonify(
                    Count=reviewCount,
                    Reviews=reviews,
                  ),200








@app.route('/airbnb/reviewers/', methods=['GET'])
def getReviewers():
    
    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    if 'sort_by_review_count' in request.args.keys():
        sort_by_review_count = str(request.args['sort_by_review_count'])
        
        if str.upper(sort_by_review_count) == 'ASCENDING':
            cur.execute("select * , (select count(rid) from review a where a.rid = b.rid) As ReviewCount from reviewer b Order by ReviewCount, rid")
        elif str.upper(sort_by_review_count) == 'DESCENDING':
            cur.execute("select * , (select count(rid) from review a where a.rid = b.rid) As ReviewCount from reviewer b Order by ReviewCount desc , rid")
        else:
            cur.execute("select * , (select count(rid) from review a where a.rid = b.rid) As ReviewCount from reviewer b Order by rid")
    else:
        cur.execute("select * , (select count(rid) from review a where a.rid = b.rid) As ReviewCount from reviewer b Order by rid")
    
    reviewers = []

    conn.commit() 

    reviewerRows = cur.fetchall()
    reviewerCount = len(reviewerRows)

    #access the table rows by column name 
    for reviewerRow in reviewerRows: 
        intRid = reviewerRow['rid']
        strRname = reviewerRow['rname']
        intReviewCount = reviewerRow['ReviewCount']


        reviewers.append(
                        {               
                            "Review Count": intReviewCount,
                            "Reviewer ID": intRid,
                            "Reviewer Name": strRname
                        }
                      )

    conn.close()
    return jsonify(
                    Count=reviewerCount,
                    Reviewers=reviewers,
                  ),200


@app.route('/airbnb/reviewers/<reviwerID>', methods=['GET'])
def getReviewerID(reviwerID):
    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    cur.execute("select review.*, reviewer.rname from review inner join reviewer on review.rid = reviewer.rid where review.rid = " + reviwerID + " order by review.datetime desc" )

    reviews = []

    conn.commit() 

    reviewRows = cur.fetchall()
    reviewCount = len(reviewRows)

    #access the table rows by column name 
    for reviewRow in reviewRows: 
        intReviewID = reviewRow['id']
        intRid = reviewRow['rid']
        strComment = reviewRow['comment']
        strDatetime = reviewRow['Datetime']
        intAccommodation_id = reviewRow['accommodation_id']
        strRname = reviewRow['rname']

        reviews.append(
                        {               
                            "Accommodation ID": intAccommodation_id,
                            "Comment": strComment,
                            "DateTime": strDatetime,
                        }
                      )

    conn.close()

    if reviewCount > 0 :
        return jsonify({
                        "Reviewer ID":intRid,
                        "Reviewer Name":strRname,
                        "Reviews":reviews,
        }),200
    else:
        return jsonify({
                "Reason": [{"Message":"Reviewer not found"}]
        }),404

@app.route('/airbnb/hosts/', methods=['GET'])
def getHosts():
    
    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    if 'sort_by_accommodation_count' in request.args.keys():
        sort_by_accommodation_count = str(request.args['sort_by_accommodation_count'])
        
        if str.upper(sort_by_accommodation_count) == 'ASCENDING':
            cur.execute("select a.host_id, a.host_about, a.host_location, a.host_name, a.host_url"
            ", (select count(*) from host_accommodation b where b.host_id = a.host_id) As AccommodationCount FROM host a inner join host_accommodation on a.host_id = host_accommodation.host_id"
            " Group by a.host_id, a.host_about, a.host_location, a.host_name, a.host_url Order by AccommodationCount asc, a.host_id")
        elif str.upper(sort_by_accommodation_count) == 'DESCENDING':
            cur.execute("select a.host_id, a.host_about, a.host_location, a.host_name, a.host_url"
                        ", (select count(*) from host_accommodation b where b.host_id = a.host_id) As AccommodationCount FROM host a inner join host_accommodation on a.host_id = host_accommodation.host_id"
                        " Group by a.host_id, a.host_about, a.host_location, a.host_name, a.host_url Order by AccommodationCount desc, a.host_id")
        else:
            cur.execute("select a.host_id, a.host_about, a.host_location, a.host_name, a.host_url"
            ", (select count(*) from host_accommodation b where b.host_id = a.host_id) As AccommodationCount FROM host a inner join host_accommodation on a.host_id = host_accommodation.host_id"
            " Group by a.host_id, a.host_about, a.host_location, a.host_name, a.host_url Order by a.host_id")
    else:
        cur.execute("select a.host_id, a.host_about, a.host_location, a.host_name, a.host_url"
            ", (select count(*) from host_accommodation b where b.host_id = a.host_id) As AccommodationCount FROM host a inner join host_accommodation on a.host_id = host_accommodation.host_id"
            " Group by a.host_id, a.host_about, a.host_location, a.host_name, a.host_url Order by a.host_id")
    
    hosts = []

    conn.commit() 

    hostRows = cur.fetchall()
    hostCount = len(hostRows)

    #access the table rows by column name 
    for hostRow in hostRows:
        intAccommodationCount = hostRow["AccommodationCount"]
        strHostAbout = hostRow['host_about']
        intHostId = hostRow['host_id']
        strHostName = hostRow['host_name']
        strHostLocation = hostRow['host_location']
        strHostUrl = hostRow['host_url']

        hosts.append(
                        {               
                            "Accommodation Count": intAccommodationCount,
                            "Host About": strHostAbout,
                            "Host ID": intHostId,
                            "Host Location": strHostLocation,
                            "Host Name": strHostName,
                            "Host URL": strHostUrl
                        }
                      )

    conn.close()
    return jsonify(
                    Count=hostCount,
                    Hosts=hosts,
                  ),200










@app.route('/airbnb/hosts/<hostID>', methods=['GET'])
def gethostID(hostID):
    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    cur.execute("select *, (select count(*) from host_accommodation b where b.host_id = a.host_id) As AccommodationCount FROM host a"
                " WHERE a.host_id = " + hostID)

    hosts = []

    conn.commit() 

    hostsRows = cur.fetchall()
    hostsCount = len(hostsRows)

    #access the table rows by column name 
    for hostRow in hostsRows: 
        intAccommodationCount = hostRow["AccommodationCount"]
        strHostAbout = hostRow['host_about']
        intHostId = hostRow['host_id']
        strHostName = hostRow['host_name']
        strHostLocation = hostRow['host_location']
        strHostUrl = hostRow['host_url']

        conn2 = sqlite3.connect("airbnb.db") 
        conn2.row_factory = sqlite3.Row
        cur2 = conn2.cursor() 

        cur2.execute("SELECT host_accommodation.accommodation_id, accommodation.name FROM host_accommodation inner join accommodation"
                    " on host_accommodation.accommodation_id = accommodation.id where host_accommodation.host_id =" + hostID + " ORDER BY host_accommodation.accommodation_id")

        conn2.commit() 
        host_accommodationRows = cur2.fetchall()
        accommodationCount = len(host_accommodationRows)

        accommodation = []
        for host_accommodationRow in host_accommodationRows: 
            intAccommodation_id = host_accommodationRow['accommodation_id']
            strAccommodationName = host_accommodationRow['name']

            accommodation.append(
                            {               
                                "Accommodation ID": intAccommodation_id,
                                "Accommodation Name": strAccommodationName,
                            }
                        )
        conn2.close

    conn.close()

    if hostsCount > 0 :
        return jsonify({
                        "Accommodation":accommodation,
                        "Accommodation Count":accommodationCount,
                        "Host About": strHostAbout,
                        "Host ID": intHostId,
                        "Host Location": strHostLocation,
                        "Host Name": strHostName,
                        "Host URL": strHostUrl
        }),200
    else:
        return jsonify({
                "Reason": [{"Message":"Host not found"}]
        }),404








@app.route('/airbnb/accommodations/', methods=['GET'])
def getAccommodations():
    
    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    strWhereMinReviewScoreValue = ""
    strWhereFilterAmenities = ""

    if 'min_review_score_value' in request.args.keys():
        intMinReviewScoreValue = int(request.args['min_review_score_value'])
        strWhereMinReviewScoreValue = " AND accommodation.review_score_value >= " + str(intMinReviewScoreValue)

    if 'amenities' in request.args.keys():
        strFilterAmenities = str(request.args['amenities'])
        strWhereFilterAmenities = " AND amenities.type ='" + strFilterAmenities + "'"

    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    cur.execute("select distinct accommodation.id From accommodation inner join amenities on amenities.accommodation_id = accommodation.id"
                " WHERE 1=1" + strWhereMinReviewScoreValue + strWhereFilterAmenities +
                " ORDER BY accommodation.id")

    conn.commit() 

    accommodationRows = cur.fetchall()
    accommodationCount = len(accommodationRows)
    Accommodations = []

    #access the table rows by column name 
    for accommodationRow in accommodationRows:
        intAccommodationId = accommodationRow["id"]

        conn2 = sqlite3.connect("airbnb.db") 
        conn2.row_factory = sqlite3.Row
        cur2 = conn2.cursor() 

        cur2.execute("select *, (select count(*) from review b where b.accommodation_id = a.id) As ReviewCount "
                    " From accommodation a inner join host_accommodation on a.id = host_accommodation.accommodation_id "
                    " inner join host on host.host_id = host_accommodation.host_id "
                    " inner join amenities on amenities.accommodation_id = a.id "
                    " where a.id = " + str(intAccommodationId))

        conn2.commit() 
        resultRows = cur2.fetchall()
        resultCount = len(resultRows)

        Accommodation = []
        Amenities = []
        Host = []

        for resultRow in resultRows: 
            intAccommodation_id = resultRow['accommodation_id']
            strAccommodationName = resultRow['name']
            strAccommodationSummary = resultRow['Summary']
            strAccommodationUrl = resultRow['url']

            strHostAbout = resultRow['host_about']
            intHostId = resultRow['host_id']
            strHostLocation = resultRow['host_location']
            strHostName = resultRow['host_name']

            intReviewCount = resultRow['ReviewCount']
            intReviewScoreValue = resultRow['review_score_value']

            strAmenities = resultRow['type']

            Amenities.append(strAmenities)
        conn2.close

        Host.append(
                            {               
                                "About": strHostAbout,
                                "ID": intHostId,
                                "Location": strHostLocation,
                                "Name": strHostName
                            }
                        )
        
        Accommodations.append(
                            {               
                                "Accommodation":{              
                                                "Name": strAccommodationName,
                                                "Summary": strAccommodationSummary,
                                                "URL": strAccommodationUrl
                                                },
                                "Amenities": Amenities,
                                "Host":{               
                                        "About": strHostAbout,
                                        "ID": intHostId,
                                        "Location": strHostLocation,
                                        "Name": strHostName
                                        },
                                "ID": intAccommodation_id,
                                "Review Count": intReviewCount,
                                "Review Score Value:": intReviewScoreValue
                            }
                        )
    conn.close()
    return jsonify({
                "Accommodations":Accommodations,
                "Count": accommodationCount
    }),200





@app.route('/airbnb/accommodations/<accommodationID>', methods=['GET'])
def getAccommodationID(accommodationID):
    import sqlite3
    conn = sqlite3.connect("airbnb.db") 

    conn.row_factory = sqlite3.Row
    cur = conn.cursor() 

    cur.execute("select * from accommodation where accommodation.id = " + str(accommodationID))

    accommodation = []

    conn.commit() 

    accommodationsRows = cur.fetchall()
    accommodationsCount = len(accommodationsRows)

    #access the table rows by column name 
    for accommodationRows in accommodationsRows: 
        intAccommodationId = accommodationRows["id"]
        strAccommodationName = accommodationRows["name"]
        intAccommodationReviewScoreValue = accommodationRows["review_score_value"]
        strAccommodationSummary = accommodationRows["Summary"]
        strAccommodationUrl = accommodationRows["url"]

        conn2 = sqlite3.connect("airbnb.db") 
        conn2.row_factory = sqlite3.Row
        cur2 = conn2.cursor() 

        cur2.execute("SELECT * FROM amenities Where accommodation_id =" + str(accommodationID))

        conn2.commit() 
        amenitiesRows = cur2.fetchall()
        amenitiesCount = len(amenitiesRows)

        amenities = []
        for amenitiesRow in amenitiesRows: 
            strAmenitiesType = amenitiesRow['type']

            amenities.append(strAmenitiesType)
        conn2.close

        #Reviews
        conn2 = sqlite3.connect("airbnb.db") 
        conn2.row_factory = sqlite3.Row
        cur2 = conn2.cursor() 

        cur2.execute("SELECT * FROM review inner join reviewer on review.rid = reviewer.rid Where accommodation_id =" + str(accommodationID))

        conn2.commit() 
        reviewsRows = cur2.fetchall()
        reviewsCount = len(reviewsRows)

        reviews = []
        for reviewsRow in reviewsRows: 
            strReviewComment = reviewsRow['comment']
            strReviewDateTime = reviewsRow['datetime']
            strReviewerId = reviewsRow['rid']
            strReviewerName = reviewsRow['rname']

            reviews.append(
                            {               
                                "Comment": strReviewComment,
                                "DateTime": strReviewDateTime,
                                "Reviewer ID": strReviewerId,
                                "Reviewer Name": strReviewerName
                            }
                        )
        conn2.close

    conn.close()

    if accommodationsCount > 0 :
        return jsonify({
                        "Accommodation ID":intAccommodationId,
                        "Accommodation Name":strAccommodationName,
                        "Amenities": amenities,
                        "Review Score Value": intAccommodationReviewScoreValue,
                        "Reviews": reviews,
                        "Summary": strAccommodationSummary,
                        "URL": strAccommodationUrl
        }),200
    else:
        return jsonify({
                "Reason": [{"Message":"Accommodation not found"}]
        }),404


    



        

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


