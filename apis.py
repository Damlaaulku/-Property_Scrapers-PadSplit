from redfin_custom import Redfin
import googlemaps
import requests
import os
from dotenv import load_dotenv
load_dotenv()

def closestStation(addr,stopType):
    GMAPS_API_KEY = os.environ["GMAPS_API_KEY"]
    gmaps = googlemaps.Client(key=GMAPS_API_KEY)
    geo_response = gmaps.geocode(addr)
    city=""
    state=""
    country=""
    try:
        components=geo_response[0]["address_components"]   
        for i in range(0,len(components)):
            if components[i]["types"][0]=="locality":
                city=components[i]["long_name"]
            elif components[i]["types"][0]=="administrative_area_level_1":
                state=components[i]["short_name"]
            elif components[i]["types"][0]=="administrative_area_level_2":
                country=components[i]["long_name"]
        c_orig = geo_response[0]['geometry']['location']  
        nearby_bus_stops = gmaps.places_nearby(location=(c_orig['lat'], c_orig['lng']), keyword=stopType, rank_by='distance')
        if nearby_bus_stops['status'] == 'ZERO_RESULTS':
            return 'ZERO_RESULTS'
      
        c_dest = nearby_bus_stops['results'][0]['geometry']['location']
        result = gmaps.distance_matrix(c_orig, c_dest, mode='walking')
        walking_dist = result['rows'][0]['elements'][0]['distance']['value']
        walking_stop_duration = result['rows'][0]['elements'][0]['duration']['text']
        retVal = str(walking_stop_duration) + "," + str("{:.2f}".format(walking_dist*0.000621371192)) + "," + city + "," + state + "," + country
    except:
        walking_dist = ""
        walking_stop_duration = ""
        retVal = walking_dist + "," + walking_stop_duration + "," + city + "," + state + "," + country
    
    return retVal

def redFin(address):
    
    amenitiesDict = {'basement_sq_feet':"",'basement_type':"",'numberof_full_baths':"",'numberof_half_baths':"",'subdivision_name':"",'living_sq_feet':"",'groundfloor_sq_feet':"",	
    'amenities_num_stories':"",'amenities_stories_type':"",'num_parking_spaces':"",'parking_type':"",'garage_type':"",
    'lot_sqft':"",'lot_acreage':"",'sewer_type':"",'water_type':"",'flood_zone':"",'num_stories':"",'year_built':"",
    'sqft_finished':"",'total_sqft':"",'taxes_rollYear':"",'taxes_due':"",'history_marketing_remarks':"",'num_beds':"",
    'num_baths':"",'property_estimate':"",'elementaryS_name':"",'elementaryS_rating':"",'elementaryS_distance':"",'middleS_name':"",
    'middleS_rating':"",'middleS_distance':"",'highS_name':"",'highS_rating':"",'highS_distance':"",'rental_estimate_low':"",
    'rental_estimate_high':""}

    client = Redfin()
    response = client.search(address)
    payloads = response['payload']

    if 'exactMatch' in payloads:
        url = payloads['exactMatch']['url']
        initial_info = client.initial_info(url)
        initpayloads = initial_info['payload']

        if 'propertyId' in initpayloads:
            property_id = initpayloads['propertyId']
            mls_data = client.below_the_fold(property_id) 

            #redFinAmenities
            pay = mls_data['payload']
            payRes = redFinAmenities(pay)
            amenitiesDict.update(payRes) 

            #redFinPublicRecords
            basicInfo = mls_data['payload']['publicRecordsInfo']
            basicDict = redFinPublicRecords(basicInfo)
            amenitiesDict.update(basicDict)
            
            #redFinPropertyHistory
            propertyHistory = mls_data['payload']['propertyHistoryInfo']
            if len(propertyHistory['events'])>0:
                marketing_remarks = propertyHistory['events'][0]
                if 'marketingRemarks' in marketing_remarks:
                    history_marketing_remarksarr = marketing_remarks['marketingRemarks']
                    if len(history_marketing_remarksarr)>0:
                        history_marketing_remarksf = history_marketing_remarksarr[0]
                        if 'marketingRemark' in history_marketing_remarksf:
                            history_marketing_remarks = history_marketing_remarksf['marketingRemark']
                            amenitiesDict['history_marketing_remarks'] = str(history_marketing_remarks)
            
            #redFinSchools
            schoolsAndDistrict =  mls_data['payload']['schoolsAndDistrictsInfo']
            keyE = 'elementarySchools'
            Eschools = redFinSchoolsAndDistrict(schoolsAndDistrict,keyE)
            amenitiesDict.update(Eschools)
            keyM = 'middleSchools'
            Mschools = redFinSchoolsAndDistrict(schoolsAndDistrict,keyM)
            amenitiesDict.update(Mschools)
            keyH = 'highSchools'
            Hschools = redFinSchoolsAndDistrict(schoolsAndDistrict,keyH)
            amenitiesDict.update(Hschools)

            if 'listingId' in initpayloads:
                listing_id = initpayloads['listingId']
                avm_details = client.avm_details(property_id, listing_id)  

                #floodzone
                fz = client.flood_zone(property_id, listing_id)
                if 'payload' in fz:
                    flood_zonepl = fz['payload']
                    if 'femaZones' in flood_zonepl:
                        flood_zone = flood_zonepl['femaZones']
                        amenitiesDict['flood_zone'] = str(flood_zone)

                #redFinAvmdetails
                if 'payload' in avm_details:
                    avmdetails = avm_details['payload']  
                    if 'numBeds' in avmdetails:
                        num_beds_redfin = avmdetails['numBeds'] 
                        amenitiesDict['num_beds'] = str(num_beds_redfin)          
                    if 'numBaths' in avmdetails:
                        num_baths_redfin = avmdetails['numBaths']
                        amenitiesDict['num_baths'] = str(num_baths_redfin)               
                    if 'predictedValue' in avmdetails:
                        property_estimate_redfin = avmdetails['predictedValue'] 
                        amenitiesDict['property_estimate'] = "$" + str(property_estimate_redfin)    
                            
                re = client.rental_estimate(property_id, listing_id)
                rePayloads = re['payload']
                if 'rentalEstimateInfo' in rePayloads:
                    rentalInfo = rePayloads['rentalEstimateInfo']
                    if 'predictedValueLow' in rentalInfo:
                        rental_estimate_low_redfin = rentalInfo['predictedValueLow']
                        amenitiesDict['rental_estimate_low'] = "$" + str(rental_estimate_low_redfin)
                    if 'predictedValueHigh' in rentalInfo:
                        rental_estimate_high_redfin = rentalInfo['predictedValueHigh']
                        amenitiesDict['rental_estimate_high'] = "$" + str(rental_estimate_high_redfin)          
    return amenitiesDict
def redFinAmenities(pay):
    amenitiesDict = dict()

    if 'amenitiesInfo' in pay and pay['amenitiesInfo']['totalAmenities'] > 0:
        amenities = pay['amenitiesInfo']
        superGroups = amenities['superGroups']
        for i in range(0,len(superGroups)):
            if superGroups[i]['types'][0]== 21:
                amenityGroups = superGroups[i]['amenityGroups']
                amenitylen = len(amenityGroups)
                for j in range(0,amenitylen):
                    if amenityGroups[j]['groupTitle']== 'Basement Information':
                        proamenityEntries = amenityGroups[j]['amenityEntries']
                        proamenityEntrieslen = len(proamenityEntries)
                        for k in range(0,proamenityEntrieslen):
                            if proamenityEntries[k]['amenityName'] == 'Basement Sq. Ft':
                                basement_sq_feet=proamenityEntries[k]['amenityValues'][0]      
                                amenitiesDict['basement_sq_feet'] = str(basement_sq_feet)
                            if proamenityEntries[k]['amenityName'] == 'Basement Type':
                                basement_type=proamenityEntries[k]['amenityValues'][0]      
                                amenitiesDict['basement_type'] = str(basement_type)                     
                    if amenityGroups[j]['groupTitle']== 'Bathroom Information':
                        proamenityEntries = amenityGroups[j]['amenityEntries']
                        proamenityEntrieslen = len(proamenityEntries)
                        for k in range(0,proamenityEntrieslen):
                            if proamenityEntries[k]['amenityName'] == '# of Full Baths':
                                numberof_full_baths=proamenityEntries[k]['amenityValues'][0]   
                                amenitiesDict['numberof_full_baths'] = str(numberof_full_baths)  
                            if proamenityEntries[k]['amenityName'] == '# of 1/2 Baths':
                                numberof_half_baths=proamenityEntries[k]['amenityValues'][0]   
                                amenitiesDict['numberof_half_baths'] = str(numberof_half_baths)                        
            if superGroups[i]['types'][0]== 20:
                amenityGroups = superGroups[i]['amenityGroups']
                amenitylen = len(amenityGroups)
                for j in range(0,amenitylen):
                    if amenityGroups[j]['groupTitle']== 'Property Information':
                        proamenityEntries = amenityGroups[j]['amenityEntries']
                        proamenityEntrieslen = len(proamenityEntries)
                        for k in range(0,proamenityEntrieslen):
                            if proamenityEntries[k]['amenityName'] == 'Subdivision Name':
                                subdivision_name=proamenityEntries[k]['amenityValues'][0]   
                                amenitiesDict['subdivision_name'] = str(subdivision_name)                        
                            if proamenityEntries[k]['amenityName'] == 'Living Sq. Ft':
                                living_sq_feet=proamenityEntries[k]['amenityValues'][0]   
                                amenitiesDict['living_sq_feet'] = str(living_sq_feet)                
                            if proamenityEntries[k]['amenityName'] == 'Ground Floor Sq. Ft':
                                groundfloor_sq_feet=proamenityEntries[k]['amenityValues'][0] 
                                amenitiesDict['groundfloor_sq_feet'] = str(groundfloor_sq_feet)                   
                            if proamenityEntries[k]['amenityName'] == '# of Stories':
                                amenities_num_stories=proamenityEntries[k]['amenityValues'][0]
                                amenitiesDict['amenities_num_stories'] = str(amenities_num_stories)                         
                            if proamenityEntries[k]['amenityName'] == 'Stories Type':
                                amenities_stories_type=proamenityEntries[k]['amenityValues'][0]    
                                amenitiesDict['amenities_stories_type'] = str(amenities_stories_type)                      
                    if amenityGroups[j]['groupTitle']== 'Parking & Garage Information':
                        proamenityEntries = amenityGroups[j]['amenityEntries']
                        proamenityEntrieslen = len(proamenityEntries)
                        for k in range(0,proamenityEntrieslen):
                            if proamenityEntries[k]['amenityName'] == '# of Parking Spaces':
                                num_parking_spaces=proamenityEntries[k]['amenityValues'][0] 
                                amenitiesDict['num_parking_spaces'] = str(num_parking_spaces)                          
                            if proamenityEntries[k]['amenityName'] == 'Parking Type':
                                parking_type=proamenityEntries[k]['amenityValues'][0] 
                                amenitiesDict['parking_type'] = str(parking_type)   
                            if proamenityEntries[k]['amenityName'] == 'Garage / Carport Type':
                                garage_type=proamenityEntries[k]['amenityValues'][0] 
                                amenitiesDict['garage_type'] = str(garage_type)                                                          
                    if amenityGroups[j]['groupTitle']== 'Lot Information':
                        proamenityEntries = amenityGroups[j]['amenityEntries']
                        proamenityEntrieslen = len(proamenityEntries)
                        for k in range(0,proamenityEntrieslen):
                            if proamenityEntries[k]['amenityName'] == 'Land Sq. Ft':
                                lot_sqft=proamenityEntries[k]['amenityValues'][0] 
                                amenitiesDict['lot_sqft'] = str(lot_sqft).replace(",","")                         
                            if proamenityEntries[k]['amenityName'] == 'Acres':
                                lot_acreage=proamenityEntries[k]['amenityValues'][0]
                                amenitiesDict['lot_acreage'] = str(lot_acreage)  
                    if amenityGroups[j]['groupTitle']== 'Utility Information':
                        proamenityEntries = amenityGroups[j]['amenityEntries']
                        proamenityEntrieslen = len(proamenityEntries)
                        for k in range(0,proamenityEntrieslen):
                            if proamenityEntries[k]['amenityName'] == 'Sewer Type':
                                sewer_type=proamenityEntries[k]['amenityValues'][0]
                                amenitiesDict['sewer_type'] = str(sewer_type)                           
                            if proamenityEntries[k]['amenityName'] == 'Water Service Type':
                                water_type=proamenityEntries[k]['amenityValues'][0]       
                                amenitiesDict['water_type'] = str(water_type)                       
    return amenitiesDict  
def redFinPublicRecords(basicInfo):
    basicDict=dict()

    basics = basicInfo['basicInfo']
    tax = basicInfo['taxInfo']

    if 'numStories' in basics:
        num_stories = basics['numStories']
        basicDict['num_stories'] = str(num_stories)
    if 'yearBuilt' in basics:
        year_built = basics['yearBuilt']
        basicDict['year_built'] = str(year_built)
    if 'sqFtFinished' in basics:
        sqft_finished = basics['sqFtFinished']
        basicDict['sqft_finished'] = str(sqft_finished)
    if 'totalSqFt' in basics:
        total_sqft = basics['totalSqFt']
        basicDict['total_sqft'] = str(total_sqft)   
    if 'lotSqFt' in basics:
        lot_sqft = basics['lotSqFt']
        basicDict['lot_sqft'] = str(lot_sqft)
    if 'rollYear' in tax:
        taxes_rollYear = tax['rollYear']
        basicDict['taxes_rollYear'] = str(taxes_rollYear) 
    if 'taxesDue' in tax:
        taxes_due = tax['taxesDue']       
        basicDict['taxes_due'] = "$" + str(taxes_due)
    return basicDict
def redFinSchoolsAndDistrict(schoolsAndDistrict,key):
    schoolsAndDistrictDict = dict()
    
    if key=='elementarySchools':
        if 'elementarySchools' in schoolsAndDistrict:
            esArray = schoolsAndDistrict['elementarySchools']
        else:
            esArray = ''
    elif key=='middleSchools':
        if 'middleSchools' in schoolsAndDistrict:
            esArray = schoolsAndDistrict['middleSchools']
        else:
            esArray = ''
    else:
        if 'highSchools' in schoolsAndDistrict:
            esArray = schoolsAndDistrict['highSchools']
        else:
            esArray = ''
    trueArr=[]
    ename = ""
    erating = ""
    edistance = ""
    for i in range(0,len(esArray)):
        if esArray[i]['servesHome'] == True: 
            temp = dict()        
            ename = esArray[i]['name']
            erating = esArray[i]['parentRating']
            edistance = esArray[i]['distanceInMiles']
            temp['name'] = ename   
            temp['ranting'] = erating
            temp['distance'] = edistance  
            trueArr.append(temp)   
    distances=[]  
    if len(trueArr) == 0:
        for i in range(0,len(esArray)):
            dist = esArray[i]['distanceInMiles']
            distances.append(dist)
        if len(distances)>0:
            index = distances.index(min(distances))
            ename = esArray[index]['name']
            erating = esArray[index]['parentRating']
            edistance = esArray[index]['distanceInMiles']
    else:
        for i in range(0,len(trueArr)):
            dist = trueArr[i]['distance']
            distances.append(dist)
        if len(distances)>0:
            index = distances.index(min(distances))
            ename = trueArr[index]['name']
            erating = trueArr[index]['ranting']
            edistance = trueArr[index]['distance']

    if key=='elementarySchools':
        schoolsAndDistrictDict['elementaryS_name'] = str(ename)
        schoolsAndDistrictDict['elementaryS_rating'] = str(erating)
        schoolsAndDistrictDict['elementaryS_distance'] = str(edistance)
    elif key=='middleSchools':
        schoolsAndDistrictDict['middleS_name'] = str(ename)
        schoolsAndDistrictDict['middleS_rating'] = str(erating)
        schoolsAndDistrictDict['middleS_distance'] = str(edistance)
    else:   
        schoolsAndDistrictDict['highS_name'] = str(ename)
        schoolsAndDistrictDict['highS_rating'] = str(erating)
        schoolsAndDistrictDict['highS_distance'] = str(edistance)

    return schoolsAndDistrictDict

def getZillowEstimate(address):
    payload = {'limit': 1, 'access_token': 'YOURTOKEN', 'near': address}
    response = requests.get('https://api.bridgedataoutput.com/api/v2/zestimates_v2/zestimates', params=payload)
    try:
        data = response.json()
        zestimate= str()
        rentzestimate= str()
        lat= str()
        long= str()
        try:
            zestimate = data['bundle'][0]['zestimate']       
        except:
            zestimate = ""
        try:
            rentzestimate = data['bundle'][0]['rentalZestimate']      
        except:
            rentzestimate = ""
        try:
            lat = data['bundle'][0]['Latitude']      
        except:
            lat = ""
        try:
            long = data['bundle'][0]['Longitude']      
        except:
            lat = ""
        
        return str(zestimate) + "," + str(rentzestimate) + "," + str(lat) + "," + str(long)
    except:
        return " , , , "

def calculateDistance(addr):   
    GMAPS_API_KEY = os.environ["GMAPS_API_KEY"]
    gmaps = googlemaps.Client(key=GMAPS_API_KEY)
    atl= "225 Baker St. NW Atlanta, GA 30313"
    dallas = "350 North Saint Paul St. Dallas, TX 30313"
    fw="1201 Houston St. Fort Worth, TX 76201"
    houston="901 Bagby St. Houston, TX 77002"
    jack ="231 E. Forsyth St. Jacksonville, FL 32202"
    geo_response = gmaps.geocode(addr)
    try:
        components=geo_response[0]["address_components"]   
        for i in range(0,len(components)):
            if components[i]["types"][0]=="administrative_area_level_1":
                state=components[i]["short_name"]  
        if state == "GA":
            dist_res = gmaps.distance_matrix(atl, addr)
            distance = float(dist_res['rows'][0]['elements'][0]['distance']['value'])
        elif state == "FL":
            dist_res = gmaps.distance_matrix(jack, addr)
            distance = float(dist_res['rows'][0]['elements'][0]['distance']['value'])
        elif state == "TX":
            dist_res = gmaps.distance_matrix(dallas, addr)
            distance = float(dist_res['rows'][0]['elements'][0]['distance']['value'])
            minVal = distance
            dist_res = gmaps.distance_matrix(fw, addr)
            distance = float(dist_res['rows'][0]['elements'][0]['distance']['value'])
            if distance < minVal:
                minVal = dist_res
            dist_res = gmaps.distance_matrix(houston, addr)
            distance = float(dist_res['rows'][0]['elements'][0]['distance']['value'])
            if distance < minVal:
                minVal = dist_res   
        if distance < 40233.60:
            return 1
        else:
            return 0
    except:
        return 2