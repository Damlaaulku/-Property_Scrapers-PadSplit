from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from datetime import date
import requests
import db_connect
import requests
import gspread
import apis

#Offerpad (Atlanta, Houston, Dallas, Jacksonville)
def getResponse():
    api_key = 'pgCQHznoxw79rPKTH5ftOKnHEbJkN1tIteqEsIWU8BF4Nl6e'
    datesdict={'unique_id':"",'created_at':"",'updated_at':"",'resurface_reason':""}
    response = requests.post(
        'https://offerpad.direct/api/get-properties',
        headers={'Accept': 'application/json', 'show': 'all'},
        auth=('YOURAPIKEY', api_key)
    )
    results = response.json()
    allproperties = results['results']
    properties=list()
    for data in allproperties:
        market = data['market']
        if market=="Atlanta" or market=="Houston" or market=="Dallas" or market=="Jacksonville":
            temp = {}
            temp.update(data)
            temp.update(datesdict)
            properties.append(temp)
    return properties

def newRecControl():
    properties=getResponse()
    connection = db_connect.get_connection()
    cur = connection.cursor()
    cur.execute("SELECT id,created_at,starting_price from ort.offerpad_properties")
    rows = cur.fetchall()
    cur.execute("SELECT unique_id from ort.all_properties")
    uniqs = cur.fetchall()
    if len(uniqs)==0:
        unique_id=1
    elif len(uniqs)>0:
        uniqids=[]
        for uniq in uniqs:
            uniqids.append(int(uniq[0]))
        unique_id = int(max(uniqids)) +1
    today = date.today().strftime("%m/%d/%Y") 
    refuses=list()

    for row in rows:   
        temp=0   
        cdate = datetime.strptime(str(row[1]), '%m/%d/%Y')      
        td = datetime.strptime(today, '%m/%d/%Y')  
        daysnum=(td-cdate).days      
        if int(daysnum)==45 or int(daysnum)==90:
            temp=1  
        for prop in properties:
            proID = int(prop['id'])
            newPrice=float(prop['starting_price'])
            if proID == int(row[0]):
                createdDate=row[1]
                oldPrice=float(str(row[2]).replace("$",""))
                if createdDate == today: 
                    if newPrice < oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price reduction"
                        refuses.append(prop)
                    elif newPrice > oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price increase"
                        refuses.append(prop)
                    del properties[properties.index(prop)]
                else:
                    if newPrice < oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price reduction"
                        refuses.append(prop)
                        del properties[properties.index(prop)]
                    elif newPrice > oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price increase"
                        refuses.append(prop)
                        del properties[properties.index(prop)]
                    elif temp==1:
                        prop['created_at']=createdDate
                        prop['updated_at']=today                    
                        prop['resurface_reason']="more than 45 days or 90 days"
                        refuses.append(prop)    
                        del properties[properties.index(prop)]
    #Filtering
    masterdata=[]
    for d in properties:
        if float(d['starting_price'])>=100000 and float(d['starting_price'])<=270000:
            if float(d['beds_num'])>2 and float(d['built']) > 1940:
                Isdistance = apis.calculateDistance(d['address'])
                if Isdistance == 1:          
                    masterdata.append(d)
    for line in masterdata:
        line['created_at']=today                  
    for line in refuses:
        masterdata.append(line)
    for line in masterdata:
        line['unique_id']=unique_id
        unique_id = unique_id+1
    connection.commit()
    connection.close()
    return masterdata  
  
def insert_data():
    #Connecting to database 
    connection = db_connect.get_connection()
    cur = connection.cursor()

    #Connecting to googlesheets
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('client_services.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open('High Value Acq Targets: All Markets')
    #spreadsheet = client.open('property_scraper')
    worksheet = spreadsheet.worksheet("All_properties")

    newRecList = newRecControl()  
    stationTypes=['bus stop','subway stop']
    for line in newRecList:
        allFieldsdict={'unique_id':"",'address':"",'city':"",'zip_code':"",'price':"",'orig_list_price':"",'status':"",'days_on_market':"",'total_days_on_market':"",'current_price_per_sqft':"",'num_beds':"",'num_baths':"",'numberof_full_baths':"",'numberof_half_baths':"",'year_built':"",'living_sq_feet':"",
        'groundfloor_sq_feet':"",'sqft_finished':"",'total_sqft':"",'interior_sq_ft':"",'building_sqft':"",'heated_area':"",'amenities_num_stories':"",'num_stories':"",'num_parking_spaces':"",'numof_garage_spaces':"",'lot_sqft':"",'lot_acreage':"",'lot_size':"",'elementaryS_name':"",'highS_name':"",
        'subdivision_name':"",'has_hoa':"",'has_hoa_leasing_restrictions':"",'hoa_fee_usd':"",'hoa_frequency':"",'hoa_rules':"",'pre_uw_annual_hoa_fee_est_usd':"",'association_yes_or_no':"",'association_fee':"",'association_fee_frequency':"",'hoa_rent_restrictions':"",'master_association_fee':"",'hoa_dues':"",
        'hoa_mandatory':"",'maint_fee_amt':"",'maint_fee_pay_schedule':"",'restrictions':"",'other_mandatory_fee':"",'fee_other_amount':"",'fee_other':"",'hoa_fee_requirement':"",'taxes_due':"",'pre_uw_annual_property_tax_est_usd':"",'tax_amount':"",'scrape_source':"",'mlsNumber_or_id':"",
        'created_at':"",'updated_at':"",'resurface_reason':"",'bus_station_distance':"",'bus_station_duration':"",'subway_station_distance':"",'subway_station_duration':"",'basement_sq_feet':"",'basement_type':"",'amenities_stories_type':"",'parking_type':"",'garage_type':"",'sewer_type':"",'water_type':"",
        'flood_zone':"",'taxes_rollYear':"",'history_marketing_remarks':"",'elementaryS_rating':"",'elementaryS_distance':"",'middleS_name':"",'middleS_rating':"",'middleS_distance':"",'highS_rating':"",'highS_distance':"",'rental_estimate_low':"",'rental_estimate_high':"",'state':"",'county':"",'latitude':"",
        'longitude':"",'roof':"",'construction':"",'has_well':"",'solar_panels':"",'use_code':"",'apn':"",'pool':"",'legal_desc':"",'rental_avm_value':"",'rental_avm_high':"",'rental_avm_low':"",'gross_yield':"",'hoa_amount':"",'images_url':"",'details_url':"",'offer_url':"",'change_date':"",
        'auction_end_time':"",'auction_start_time':"",'dwelling_type':"",'estimated_current_yield':"",'flooring_type':"",'floors':"",'has_addition':"",'has_garage_conversion':"",'has_known_foundation_issues':"",'kitchen_appliance_type':"",'kitchen_countertop':"",'opendoor_brokerage_pre_underwritten':"",
        'pre_uw_annual_insurance_expense_est_usd':"",'pre_uw_annual_ongoing_repair_expense_est_usd':"",'pre_uw_annual_property_mgmt_fee_est_usd':"",'pre_uw_annual_rent_amount_high_est_usd':"",'pre_uw_annual_rent_amount_low_est_usd':"",'pre_uw_annual_rent_amount_mid_est_usd':"",'pre_uw_annual_vacancy_expense_est_usd':"",'pre_uw_cap_rate_high_est':"",
        'pre_uw_cap_rate_low_est':"",'pre_uw_cap_rate_mid_est':"",'pre_uw_home_valuation_high_est_usd':"",'pre_uw_home_valuation_low_est_usd':"",'pre_uw_home_valuation_mid_est_usd':"",'pre_uw_repair_amount_high_est_usd':"",'pre_uw_repair_amount_low_est_usd':"",'pre_uw_repair_amount_mid_est_usd':"",'product_type':"",'sherlock_assessment':"",
        'suggested_offer':"",'carport_spaces':"",'block':"",'area':"",'structure_type':"",'sold_price':"",'closed_date':"",'zestimate':"",'rentzestimate':"",
        'property_estimate':"",'original_date':""}
        address = line['address']
        zip=str(line['zip'])
        location = address + ", " + zip
        #redfin
        redfinResponse = apis.redFin(address)
        allFieldsdict.update(redfinResponse)
        if allFieldsdict["total_sqft"]=="" or int(allFieldsdict["total_sqft"])>1000:
            #google
            timeVal=str()
            dist=str()
            for type in stationTypes:
                retVal = apis.closestStation(location,type)
                if retVal != 'ZERO_RESULTS' :    
                    mins = retVal.split(",")
                    timeValarr=mins[0].split(" ")
                    if len(timeValarr)==2:
                        timeVal=int(timeValarr[0])
                    elif len(timeValarr)==4:
                        timeVal=int(timeValarr[0])*60+int(timeValarr[2])
                    dist=mins[1]
                    if type=='bus stop':
                        allFieldsdict['bus_station_duration']=timeVal
                        allFieldsdict['bus_station_distance']=dist
                    else:
                        allFieldsdict['subway_station_duration']=timeVal
                        allFieldsdict['subway_station_distance']=dist
            #offerpad fields
            allFieldsdict['unique_id']=line['unique_id']
            allFieldsdict['scrape_source']="offerpad"
            allFieldsdict['created_at']=line['created_at']
            allFieldsdict['updated_at']=line['updated_at']
            allFieldsdict['resurface_reason']=line['resurface_reason']
            allFieldsdict['address']=address
            allFieldsdict['zip_code']=zip
            allFieldsdict['original_date']=line['original_date']
            allFieldsdict['city']=line['city']
            allFieldsdict['state']=line['state']
            allFieldsdict['roof']=line['roof']
            allFieldsdict['apn']=line['apn']
            allFieldsdict['pool']=line['pool']
            allFieldsdict['legal_desc']=line['legal_desc']
            allFieldsdict['hoa_frequency']=line['hoa_freq']
            allFieldsdict['images_url']=line['images_url']
            allFieldsdict['details_url']=line['details_url']
            allFieldsdict['offer_url']=line['offer_url']
            allFieldsdict['change_date']=line['change_date']
            allFieldsdict['construction']=line['construction']
            allFieldsdict['solar_panels']=line['solar_panels']
            allFieldsdict['use_code']=line['use_code']
            allFieldsdict['mlsNumber_or_id']=line['id']
            allFieldsdict['lot_size']=line['lot_size']
            allFieldsdict['tax_amount']=line['tax_amount']
            allFieldsdict['taxes_rollYear']=line['tax_year']
            allFieldsdict['suggested_offer']=line['suggested_offer']
            if line['starting_price'] != "":
                allFieldsdict['price']="$" + str(line['starting_price']) 
            allFieldsdict['county']=str(line['county']) +" County"
            allFieldsdict['latitude']=line['latitude']
            allFieldsdict['longitude']=line['longitude']            
            allFieldsdict['has_well']=line['has_well']     
            allFieldsdict['total_sqft']=line['sqft']  
            allFieldsdict['num_beds']=line['beds_num']
            allFieldsdict['num_baths']=line['baths_num']
            allFieldsdict['year_built']=line['built']
            if allFieldsdict['subdivision_name']=="":
                allFieldsdict['subdivision_name']=line['subdivision']
            if line['rental_avm_value'] != "":     
                allFieldsdict['rental_avm_value']="$" + str(line['rental_avm_value'])
            if line['rental_avm_high'] != "":
                allFieldsdict['rental_avm_high']="$" + str(line['rental_avm_high'])
            if line['rental_avm_low'] != "":
                allFieldsdict['rental_avm_low']="$" + str(line['rental_avm_low'])
            allFieldsdict['gross_yield']=line['gross_yield']    
            if line['hoa_amount'] != "":  
                allFieldsdict['hoa_amount']="$" + str(line['hoa_amount']) 
            if line['parking_type'] != "":
                allFieldsdict['parking_type']=line['parking_type']
            if line['parking_spaces'] != "":
                allFieldsdict['num_parking_spaces']=line['parking_spaces']
            if line['levels'] != "":
                allFieldsdict['num_stories']=line['levels']      
            if line['has_hoa']=="1":
                allFieldsdict['has_hoa']="Yes" 
            if line['has_hoa']=="0":
                allFieldsdict['has_hoa']="None"     
            #zillow
            zillowResp= apis.getZillowEstimate(address).split(',')
            zestimate=zillowResp[0]
            rentzestimate=zillowResp[1]
            allFieldsdict['zestimate']=zestimate
            allFieldsdict['rentzestimate']=rentzestimate
            if allFieldsdict["total_sqft"]=="" or int(allFieldsdict["total_sqft"])>1000:
                if line['resurface_reason'] == "price reduction" or line['resurface_reason'] == "price rising":
                    cur.execute(
                        f"delete from ort.offerpad_properties where id='{line['id']}'"
                    )
                    connection.commit()
                #offerpad table
                dbvalues = list()
                dbvalues.append(allFieldsdict['mlsNumber_or_id'])
                dbvalues.append(allFieldsdict['price'])
                dbvalues.append(allFieldsdict['created_at'])
                cur.execute(
                    f"insert into ort.offerpad_properties values ({', '.join(['%s'] * len(dbvalues))})",
                    dbvalues,
                )
                #all table and sheet
                values=list(allFieldsdict.values())
                worksheet.append_row(values, value_input_option='USER_ENTERED')
                cur.execute(
                    f"insert into ort.all_properties values ({', '.join(['%s'] * len(values))})",
                    values,
                )
            connection.commit()       
    connection.close()
insert_data()