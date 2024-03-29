#  This code will loop through Tableau Public profiles and write each one's stats to its own Google Sheet.
#  Written by Ken Flerlage, February, 2021.

# Add fields for viz thumbnail and featured viz thumbnail.

import sys
import json
from weakref import ref
import requests
import datetime
import gspread
import time
import boto3
from oauth2client.service_account import ServiceAccountCredentials
from botocore.exceptions import ClientError

# Max runtime, in seconds, before exiting the program to avoid exceeding lambda max runtimes (900 seconds)
maxRuntime = 780 

# Other Variables
urlProfileWB = 'https://public.tableau.com/public/apis/workbooks'

# Fill in the following values with your own information.
senderAddress = "Sender Name <email address>"                           # From name/email address for emails.
ownerAddress = "email address"                                          # From email address for emails.
s3Bucket = "bucket name"                                                # Name of the S3 bucket containing the credentials file.
credsFile = "creds file name"                                           # Name of the credentials file in the S3 bucket.
worksheetID = "Worksheet ID"                                            # ID of the Sign-Up Google Sheet.


#------------------------------------------------------------------------------------------------------------------------------
# Email new user
#------------------------------------------------------------------------------------------------------------------------------
def send_new_user_email (email, firstName, url):
    sender = senderAddress
    recipient = email

    region = "us-east-2"
    subject = "Tableau Public Stats Service"

    # The email body for recipients with non-HTML email clients.
    bodyText = (firstName + ",\r\n\r\n" 
        "Thank you for subscribing to the Tableau Public Stats Service. We've created a Google Sheet and populated it with your statistics. This data will be refreshed on a daily basis so that you can keep up with your ever-changing information. Your Google Sheet can be found here: " + url + ".\r\n\r\n" 
        "Feel free to use this data however you like, including building your own statistics data visualization for Tableau Public. Because Tableau Public allows automated refreshes from Google Sheets, you'll be able to keep it up-to-date automatically, without any manual intervention.\r\n\r\n" 
        "If you have any questions, concerns, or would like to unsubscribe to the service, please email Ken Flerlage at flerlagekr@gmail.com. \r\n\r\n" 
        "Thanks,\r\n"
        "Ken"
        )
                
    # The HTML body of the email.
    bodyHTML = """
    <html>
    <head></head>
    <body>
    <p style="font-family:Georgia;font-size:15px">""" + firstName + """,</p>
    <p style="font-family:Georgia;font-size:15px">Thank you for subscribing to the Tableau Public Stats Service. We've created a Google Sheet and populated it with your statistics. This data will be refreshed on a daily basis so that you can keep up with your ever-changing information. Your Google Sheet can be found here: <a href=""" + url + """>Your Tableau Public Stats</a></p>
    <p style="font-family:Georgia;font-size:15px">This data has been provided in a Google Sheet format in order to make it easier for you to create a Tableau Public workbook showing your stats. Because Tableau Public allows automated refreshes from Google Sheets, you'll be able to keep it up-to-date automatically, without any manual intervention. To make it easier for you to get started, I've created a <a href="https://public.tableau.com/profile/ken.flerlage#!/vizhome/TableauPublicStats/Stats/">Starter Workbook</a> that you can easily connect to your data. This workbook also includes a sheet called "Data Dictionary" which provides definitions of each of the fields in the Google Sheet. </p>
    <p style="font-family:Georgia;font-size:15px">To learn more about the stats service and how to connect the starter workbook to your Google Sheet, see my <a href="https://www.flerlagetwins.com/stats.html">Tableau Public Stats Service</a> blog. If you have any questions, concerns, or would like to unsubscribe to the service, please email Ken Flerlage at flerlagekr@gmail.com.</p>
    <br>
    <p style="font-family:Georgia;font-size:15px">Thanks,</p>
    <p style="font-family:Georgia;font-size:15px">Ken</p>
    </body>
    </html>
    """            

    # The character encoding for the email.
    charSet = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=region)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': charSet,
                        'Data': bodyHTML,
                    },
                    'Text': {
                        'Charset': charSet,
                        'Data': bodyText,
                    },
                },
                'Subject': {
                    'Charset': charSet,
                    'Data': subject,
                },
            },
            Source=sender,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        log (e.response['Error']['Message'])
        
    else:
        log ("Sent new user welcome email to: " + email)

#------------------------------------------------------------------------------------------------------------------------------
# Send message to Ken
#------------------------------------------------------------------------------------------------------------------------------
def phone_home (subject, msg):
    sender = senderAddress
    recipient = ownerAddress

    region = "us-east-2"

    # The email body for recipients with non-HTML email clients.
    bodyText = (msg)
                
    # The HTML body of the email.
    bodyHTML = """
    <html>
    <head></head>
    <body>
    <p style="font-family:Georgia;font-size:15px">""" + msg + """</p>
    </body>
    </html>
    """            

    # The character encoding for the email.
    charSet = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=region)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': charSet,
                        'Data': bodyHTML,
                    },
                    'Text': {
                        'Charset': charSet,
                        'Data': bodyText,
                    },
                },
                'Subject': {
                    'Charset': charSet,
                    'Data': subject,
                },
            },
            Source=sender,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        log (e.response['Error']['Message'])

#------------------------------------------------------------------------------------------------------------------------------
# Write a message to the log (or screen). When running in AWS, print will write to Cloudwatch.
#------------------------------------------------------------------------------------------------------------------------------
def log (msg):
    logTimeStamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(str(logTimeStamp) + ": " + msg)

#------------------------------------------------------------------------------------------------------------------------------
# Log a message and exit the program.
#------------------------------------------------------------------------------------------------------------------------------
def end_function(msg=''):
    if msg != '':
        log(msg)
    
    exit()

#------------------------------------------------------------------------------------------------------------------------------
# Main lambda handler
#------------------------------------------------------------------------------------------------------------------------------
def lambda_handler(event, context):
    # Get the start time so we can end the program before exceeding lambda max runtimes (900 seconds)
    startTime = datetime.datetime.now()

    # Get the Google Sheets credentials from S3
    s3 = boto3.client('s3')
    key = credsFile
    object = s3.get_object(Bucket=s3Bucket, Key=key)
    content = object['Body']
    creds = json.loads(content.read())

    # Read the Google API key from the creds file.
    scope =['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    gc = gspread.authorize(credentials) 

    # Read the sign-up sheet
    docProfiles = gc.open_by_url('https://docs.google.com/spreadsheets/d/' + worksheetID)
    sheetProfiles = docProfiles.get_worksheet(0)

    emailList = sheetProfiles.col_values(2)
    firstnameList = sheetProfiles.col_values(3)
    lastnameList = sheetProfiles.col_values(4)
    profileList = sheetProfiles.col_values(5)
    urlList = sheetProfiles.col_values(6)
    dateList = sheetProfiles.col_values(7)
    profileCount = len(emailList)

    # Initialize some variables.
    processed = False
    newCount = 0

    # Loop through all the profiles.
    for i in range(1, profileCount):
        continueLoop = True

        # Check time so we can end the program before exceeding lambda max runtimes (900 seconds)
        checkTime = datetime.datetime.now()
        dateDiff = checkTime - startTime
        secondsRunning = dateDiff.seconds

        if secondsRunning >= maxRuntime:
            end_function("Program exceeded max runtime and was forced to end.")

        refreshDateStr = ''

        # Get the last refresh date.
        if len(dateList) <= i:
            # No value. Set way back.
            refreshDateStr = "2000-01-01 00:00:00"
        elif urlList[i] == "":
            # Blank. Set way back.
            refreshDateStr = "2000-01-01 00:00:00"
        else:
            # Use the value.
            refreshDateStr = dateList[i]

        # If still blank, set back.
        if refreshDateStr == '':
            refreshDateStr = "2000-01-01 00:00:00"

        refreshDate = datetime.datetime.strptime(refreshDateStr, "%Y-%m-%d %H:%M:%S")
        dateDiff = datetime.datetime.now() - refreshDate
        hoursSinceRefresh = dateDiff.total_seconds()/3600

        if hoursSinceRefresh >= 23:
            # Get profile URL and and change it to use the API url.
            profileID = profileList[i]
            urlProfile = "https://public.tableau.com/profile/" + profileID + "#!/"
            urlProfileOriginal = urlProfile
            urlProfile = urlProfile.strip()
            urlProfile = urlProfile[0:len(urlProfile)-3]
            urlProfile = urlProfile + "/"
            urlProfile = urlProfile.replace('https://public.tableau.com/profile', 'https://public.tableau.com/profile/api')

            log ("Processing profile: " + lastnameList[i] + ", " + firstnameList[i])

            if len(urlList) <= i:
                # No value.
                processed = False
            elif urlList[i] == "":
                # Blank means this hasn't been processed. 
                processed = False
            else:
                # This has already been processed.
                processed = True

            if processed == True:
                # Just get the URL that's there and try to open it
                urlStats = urlList[i]

                try:
                    docStats = gc.open_by_url(urlStats)
                    sheetStats = docStats.get_worksheet(0)

                except:
                    msg = "Could not open the spreadsheet: " + urlStats + "."
                    log(msg)

                    subject = "Tableau Public Stats Service - Error Opening Spreadsheet"
                    phone_home(subject, msg)

                    # Report the error and let the admin look into the problem.
                    continueLoop = False
            
            if continueLoop == True:
                if processed == False:
                    # Create a new spreadsheet, and assign permissions.
                    docStats = gc.create('Stats: ' + lastnameList[i] + ', ' + firstnameList[i])
                    docStats.share(ownerAddress, perm_type='user', role='writer')
                    docStats.share(emailList[i], perm_type='user', role='reader')
                    urlStats = 'https://docs.google.com/spreadsheets/d/' + docStats.id
                    log("Created new sheet: " + urlStats)

                    sheetProfiles.update_cell(i+1, 6, urlStats)
                    sheetStats = docStats.get_worksheet(0)

                # Initialize Variables
                pageCount = 50
                index = 0
                vizCount = 0
                matrix = {}
                foundValid = 1
                startDate = datetime.date(year=1970, month=1, day=1)
                timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")

                # Start by calling the API to get user info.
                response = requests.get(urlProfile)

                try:
                    output = response.json()
                    userName = output["name"]

                    # Check for organization.
                    org_exists =  "organization" in output
                    if org_exists:
                        userOrg = output["organization"]
                    else:
                        userOrg = ""

                    # Check for bio.
                    bio_exists =  "bio" in output
                    if bio_exists:
                        bio = output["bio"]
                    else:
                        bio = ""
                    
                    # Get general information.
                    followerCount = output["totalNumberOfFollowers"]
                    totalNumberOfFollowing = output["totalNumberOfFollowing"]
                    profileName = output["profileName"]
                    searchable = output["searchable"]

                    # Check for featured viz.
                    featured_exists =  "featuredVizRepoUrl" in output
                    if featured_exists:
                        featuredVizRepoUrl = output["featuredVizRepoUrl"]
                    else:
                        featuredVizRepoUrl = ""

                    # Check for avatar URL.
                    avatar_exists =  "avatarUrl" in output
                    if avatar_exists:
                        avatarUrl = output["avatarUrl"]
                    else:
                        avatarUrl = ""

                    # Check for websites.
                    websites_exists =  "websites" in output
                    if websites_exists:
                        websites = output["websites"]
                    else:
                        websites = ""

                    # Check for address.
                    address_exists =  "address" in output
                    if address_exists:
                        address = output["address"]
                        addressJson = json.loads(address)

                        # Convert address string to json and get components
                        country_exists =  "country" in addressJson
                        state_exists =  "state" in addressJson
                        city_exists =  "city" in addressJson
                        
                        if country_exists:
                            userCountry = addressJson["country"]
                        else:
                            userCountry = ""
                        
                        if state_exists:
                            userRegion = addressJson["state"]
                        else:
                            userRegion = ""
                        
                        if city_exists:
                            userCity = addressJson["city"]
                        else:
                            userCity = ""

                    else:
                        address = ""
                        userCountry = ""
                        userRegion = ""
                        userCity = ""

                    # Loop through websites and grab the ones we want
                    facebookURL = ""
                    twitterURL = ""
                    linkedinURL = ""
                    websiteURL = ""

                    for w in websites:
                        wTitle = w["title"]
                        wURL = w["url"]

                        if wTitle == "facebook.com":
                            facebookURL = wURL
                        elif wTitle == "twitter.com":
                            twitterURL = wURL
                        elif wTitle == "linkedin.com":
                            linkedinURL = wURL
                        else:
                            websiteURL = wURL

                except Exception as e:
                    # Some error occured. Report error and exit loop.
                    msg = "Unable to process the profile, " + profileID + " via API. Error: " + str(sys.exc_info()[0]) + " - " + str(e) 
                    log (msg)

                    subject = "Tableau Public Stats Service - Error Processing Profile"
                    phone_home (subject, msg)

                    foundValid = 0

                # Call the Tableau Public workbook API in chunks and write to the Google Sheet.
                # Note: The API no longer allows public users to get a list of hidden workbooks.
                while (foundValid == 1):
                    parameters = {"count": pageCount, "start": index, "profileName": profileID, "visibility": "NON_HIDDEN"}
                    response = requests.get(urlProfileWB, params=parameters)

                    try:
                        output = response.json()

                        for o in output['contents']:    
                            # Now call the Workbook Detail API for each workbook.
                            workbookID = o['workbookRepoUrl']
                            urlWorkbook = "https://public.tableau.com/profile/api/single_workbook/" + workbookID + "?"
                            response = requests.get(urlWorkbook)
                            wbStats = response.json()

                            # Get all the information about the workbook.
                            title = wbStats['title']
                            desc = wbStats['description']
                            defaultViewRepoUrl = wbStats['defaultViewRepoUrl']
                            defaultViewName = wbStats['defaultViewName']
                            showInProfile = wbStats['showInProfile']
                            viewCount = wbStats['viewCount']
                            numberOfFavorites = wbStats['numberOfFavorites']
                            permalink = wbStats['permalink']
                            firstPublishDate = wbStats['firstPublishDate']
                            lastPublishDate = wbStats['lastPublishDate']
                            revision = wbStats['revision']
                            size = wbStats['size']

                            # Calculations and cleanup of values.
                            firstPublishDateFormatted = startDate + datetime.timedelta(milliseconds=firstPublishDate)
                            lastPublishDateFormatted = startDate + datetime.timedelta(milliseconds=lastPublishDate)
                            
                            if vizCount == 0:
                                # This is the first workbook so use initialize the date with this workbooks' date.
                                lastUserPublishDateFormatted = lastPublishDateFormatted
                            else:
                                if lastPublishDateFormatted > lastUserPublishDateFormatted:
                                    # Update the date to this more recent date.
                                    lastUserPublishDateFormatted = lastPublishDateFormatted

                            # Create the various URLs.
                            urlViz ="https://public.tableau.com/views/" + defaultViewRepoUrl.replace("/sheets","") 
                            urlVizNoVizHome = urlViz + "?:embed=y&:display_count=yes&:showVizHome=no" 
                            urlThumbnail = urlViz.replace("/views/", "/static/images/" + defaultViewRepoUrl[0:2] + "/") + "/4_3.png"
                            urlViz = urlProfileOriginal + "vizhome/" + defaultViewRepoUrl.replace("/sheets","")

                            # Store all values in an array.
                            matrix[vizCount, 0]  = workbookID
                            matrix[vizCount, 1]  = title
                            matrix[vizCount, 2]  = desc
                            matrix[vizCount, 3]  = urlViz
                            matrix[vizCount, 4]  = urlVizNoVizHome
                            matrix[vizCount, 5]  = urlThumbnail
                            matrix[vizCount, 6]  = defaultViewName
                            matrix[vizCount, 7]  = showInProfile
                            matrix[vizCount, 8]  = permalink
                            matrix[vizCount, 9]  = viewCount
                            matrix[vizCount, 10] = numberOfFavorites
                            matrix[vizCount, 11] = str(firstPublishDateFormatted)
                            matrix[vizCount, 12] = str(lastPublishDateFormatted)
                            matrix[vizCount, 13] = revision
                            matrix[vizCount, 14] = size
                            matrix[vizCount, 15] = userName
                            matrix[vizCount, 16] = profileName
                            matrix[vizCount, 17] = userOrg
                            matrix[vizCount, 18] = bio   
                            matrix[vizCount, 19] = avatarUrl
                            matrix[vizCount, 20] = searchable
                            matrix[vizCount, 21] = featuredVizRepoUrl
                            matrix[vizCount, 22] = str(lastUserPublishDateFormatted)
                            matrix[vizCount, 23] = followerCount
                            matrix[vizCount, 24] = totalNumberOfFollowing
                            matrix[vizCount, 25] = userCountry
                            matrix[vizCount, 26] = userRegion
                            matrix[vizCount, 27] = userCity
                            matrix[vizCount, 28] = websiteURL
                            matrix[vizCount, 29] = linkedinURL
                            matrix[vizCount, 30] = twitterURL
                            matrix[vizCount, 31] = facebookURL
                            matrix[vizCount, 32] = urlProfileOriginal
                            matrix[vizCount, 33] = timestamp

                            vizCount += 1
                    
                        if output['next'] == None:
                            # We're out of valid vizzes, so move on.
                                foundValid = 0
                        else:
                            # Keep going.
                            foundValid = 1

                    except Exception as e:
                        # Some error occured. Report error and exit loop.
                        msg = "Unable to process the profile, " + profileID + " via API. Error: " + str(sys.exc_info()[0]) + " - " + str(e) 
                        log (msg)

                        subject = "Tableau Public Stats Service - Error Processing Profile"
                        phone_home (subject, msg)

                        foundValid = 0

                    index += pageCount

                # Loop through the matrix and write values for a batch update to Google Sheets.
                if vizCount > 0:
                    # Update user last published date
                    for vizIndex in range(0, vizCount):
                        matrix[vizIndex, 22] = str(lastUserPublishDateFormatted)

                    # Set the cell range.
                    rangeString = "A2:AH" + str(vizCount+1)
                    cell_list = sheetStats.range(rangeString)

                    # Loop through all the cells in the range, populating from the matrix.
                    row = 0
                    column = 0

                    for cell in cell_list: 
                        cell.value = matrix[row,column]
                        column += 1
                        if (column > 33):
                            column=0
                            row += 1

                    # Clear the sheet then update in batch
                    sheetStats.clear()
                    sheetStats.update_cells(cell_list)

                    # Write the header
                    matrix = {}
                    matrix[0, 0] =  "Viz - ID"
                    matrix[0, 1] =  "Viz - Title"
                    matrix[0, 2] =  "Viz - Description"
                    matrix[0, 3] =  "Viz - URL"
                    matrix[0, 4] =  "Viz - URL (No Home)"
                    matrix[0, 5] =  "Viz - Thumbnail URL"
                    matrix[0, 6] =  "Viz - Default View"
                    matrix[0, 7] =  "Viz - Visible"
                    matrix[0, 8] =  "Viz - Permalink"
                    matrix[0, 9] =  "Viz - Views"
                    matrix[0, 10] = "Viz - Favorites"
                    matrix[0, 11] = "Viz - First Published"
                    matrix[0, 12] = "Viz - Last Published"
                    matrix[0, 13] = "Viz - Revision"
                    matrix[0, 14] = "Viz - Size"
                    matrix[0, 15] = "User - Name"
                    matrix[0, 16] = "User - Profile ID"
                    matrix[0, 17] = "User - Organization"
                    matrix[0, 18] = "User - Bio"
                    matrix[0, 19] = "User - Avatar URL"
                    matrix[0, 20] = "User - Searchable"
                    matrix[0, 21] = "User - Featured Viz"
                    matrix[0, 22] = "User - Last Published"
                    matrix[0, 23] = "User - Follower Count"
                    matrix[0, 24] = "User - Following Count"
                    matrix[0, 25] = "User - Country"
                    matrix[0, 26] = "User - State or Region"
                    matrix[0, 27] = "User - City"
                    matrix[0, 28] = "User - Website"
                    matrix[0, 29] = "User - LinkedIn"
                    matrix[0, 30] = "User - Twitter"
                    matrix[0, 31] = "User - Facebook"
                    matrix[0, 32] = "User - Tableau Public"
                    matrix[0, 33] = "Stats - Stats Last Refreshed"

                    # Set the range for the header.
                    rangeString = "A1:AH1"
                    cell_list = sheetStats.range(rangeString)

                    # Loop through all the cells in the range, populating from the matrix.
                    row = 0
                    column = 0

                    for cell in cell_list: 
                        cell.value = matrix[row,column]

                        column += 1

                    # Update in batch.
                    sheetStats.update_cells(cell_list)

                    # Finishing touches
                    rangeString = "A1:AH" + str(vizCount+1)
                    sheetStats.format(rangeString, {"verticalAlignment": "TOP"})

                    rangeString = "A1:AH1"
                    sheetStats.format(rangeString, {'textFormat': {'bold': True}})

                    sheetStats.freeze(rows=1)
                    sheetStats.update_title ("Stats")

                    log ("Wrote " + str(vizCount) + " records.")

                    # If a new user, send the welcome email.
                    if processed == False:
                        send_new_user_email(emailList[i], firstnameList[i], urlStats)
                        newCount += 1

                    # Populate the last refreshed date.
                    refreshDate = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                    sheetProfiles.update_cell(i+1, 7, refreshDate)

                else:
                    log ("No records written.")


    # Send email to Ken, indicating the number of new subscribers.
    if newCount > 0:
        msg = str(newCount) + " new subscribers have been added."
        subject = "Tableau Public Stats Service - " + str(newCount) + " New Subscribers"
        phone_home (subject, msg)


#------------------------------------------------------------------------------------------------------------------------------
# Labmda will always call the lambda handler function, so this will not get run unless you are running locally.
# This code will connect to AWS locally. This requires a credentials file in C:\Users\<Username>\.aws\
# For further details, see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
#------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    log("Code is running locally..............................................................")
    context = []
    event = {"state": "DISABLED"}
    boto3.setup_default_session(region_name="us-east-2", profile_name="default")
    lambda_handler(event, context)
