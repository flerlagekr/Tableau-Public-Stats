#  This code will loop through Tableau Public profiles and write each one's stats to its own Google Sheet.
#  Written by Ken Flerlage, February, 2021.

import sys
import json
import requests
import datetime
import gspread
import time
import boto3
from oauth2client.service_account import ServiceAccountCredentials
from botocore.exceptions import ClientError

#------------------------------------------------------------------------------------------------------------------------------
# Email new user
#------------------------------------------------------------------------------------------------------------------------------
def send_new_user_email (email, firstName, url):
    sender = "Ken Flerlage <flerlagekr@gmail.com>"
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
    <p style="font-family:Georgia;font-size:15px">Thank you for subscribing to the Tableau Public Stats Service. We've created a Google Sheet and populated it with your statistics. This data will be refreshed on a daily basis so that you can keep up with your ever-changing information. Your Google Sheet can be found here: """ + url + """</p>
    <p style="font-family:Georgia;font-size:15px">Feel free to use this data however you like, including building your own statistics data visualization for Tableau Public. Because Tableau Public allows automated refreshes from Google Sheets, you'll be able to keep it up-to-date automatically, without any manual intervention.</p>
    <p style="font-family:Georgia;font-size:15px">If you have any questions, concerns, or would like to unsubscribe to the service, please email Ken Flerlage at flerlagekr@gmail.com.</p>
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
    sender = "Ken Flerlage <flerlagekr@gmail.com>"
    recipient = "flerlagekr@gmail.com"

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
# Write a message to the log (or screen)
#------------------------------------------------------------------------------------------------------------------------------
def log (msg):
    timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    print(timestamp + ": " + msg)

#------------------------------------------------------------------------------------------------------------------------------
# Main lambda handler
#------------------------------------------------------------------------------------------------------------------------------
def lambda_handler(event, context):
    # Open Google Sheet
    scope =['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    # Read Google API key from a local json file.
    credentials = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/Ken/Documents/Ken/Blog/My Vizzes/Python/creds.json', scope)
    gc = gspread.authorize(credentials) 

    # Read the sign-up sheet
    docProfiles = gc.open_by_url('https://docs.google.com/spreadsheets/d/1WFTT7ZSXNacCMEcPzOTdf659KGFnXOVe6Y9Frc48vQs')
    sheetProfiles = docProfiles.get_worksheet(0)

    emailList = sheetProfiles.col_values(2)
    firstnameList = sheetProfiles.col_values(3)
    lastnameList = sheetProfiles.col_values(4)
    profileList = sheetProfiles.col_values(5)
    urlList = sheetProfiles.col_values(6)

    profileCount = len(emailList)

    processed = False

    newCount = 0

    # Loop through all the profiles.
    for i in range(1, profileCount):
        # Get profile URL and and change it to use the API url.
        urlProfile = profileList[i]
        urlProfile = urlProfile.strip()
        urlProfile = urlProfile[0:len(urlProfile)-3]
        urlProfile = urlProfile + "/"
        urlProfile = urlProfile.replace('https://public.tableau.com/profile', ' https://public.tableau.com/profile/api')
        urlProfile = urlProfile + 'workbooks'

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
                msg = "Could not open the spreadsheet: " + urlStats + ". This will be treated as a new profile."
                log (msg)

                subject = "Tableau Public Stats Service - Error Opening Spreadsheet"
                phone_home (subject, msg)

                processed = False
        
        if processed == False:
            # Create a new spreadsheet, and assign permissions.
            docStats = gc.create('Stats: ' + lastnameList[i] + ', ' + firstnameList[i])
            docStats.share('flerlagekr@gmail.com', perm_type='user', role='writer')
            docStats.share(emailList[i], perm_type='user', role='reader')
            urlStats = 'https://docs.google.com/spreadsheets/d/' + docStats.id
            log ("Created new sheet: " + urlStats)

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

        # Call the Tableau Public API in chunks and write to the Google Sheet.
        while (foundValid == 1):
            parameters = {"count": pageCount, "index": index}
            response = requests.get(urlProfile, params=parameters)

            try:
                output = response.json()

                for o in output:
                    # Collect viz information.
                    title = o['title']
                    viewCount = o['viewCount']
                    numberOfFavorites = o['numberOfFavorites']
                    firstPublishDate = o['firstPublishDate']
                    defaultViewRepoUrl = o['defaultViewRepoUrl']
                    showInProfile = o['showInProfile']
                    viewCount = o['viewCount']

                    # Calculations and cleanup of values.
                    firstPublishDateFormatted = startDate + datetime.timedelta(milliseconds=firstPublishDate)
                    urlViz ="https://public.tableau.com/views/" + defaultViewRepoUrl.replace("/sheets","") + "?:embed=y&:display_count=yes&:showVizHome=no" 

                    # Store all values in an array.
                    matrix[vizCount,0] = title
                    matrix[vizCount,1] = viewCount
                    matrix[vizCount,2] = numberOfFavorites
                    matrix[vizCount,3] = str(firstPublishDateFormatted)
                    matrix[vizCount,4] = showInProfile
                    matrix[vizCount,5] = urlViz
                    matrix[vizCount,6] = timestamp
                    vizCount += 1
            
                if not output:
                    # We're out of valid vizzes, so quit.
                    foundValid = 0
                else:
                    # Keep going.
                    foundValid = 1
                    index += pageCount

            except:
                # Unable to serialize the response to json. Report error and exit loop.
                msg = "Unable to process the profile, " + urlProfile + " via API. This may be an invalid profile URL."
                log (msg)

                subject = "Tableau Public Stats Service - Error Processing Profile"
                phone_home (subject, msg)

                foundValid = 0

        # Loop through the matrix and write values for a batch update to Google Sheets.
        if vizCount > 0:
            rangeString = "A2:G" + str(vizCount+1)
            cell_list = sheetStats.range(rangeString)

            row = 0
            column = 0

            for cell in cell_list: 
                cell.value = matrix[row,column]
                column += 1
                if (column > 6):
                    column=0
                    row += 1

            # Update in batch   
            sheetStats.update_cells(cell_list)

            # Write the header
            sheetStats.update_cell(1, 1, "Title")
            sheetStats.update_cell(1, 2, "Views")
            sheetStats.update_cell(1, 3, "Favorites")
            sheetStats.update_cell(1, 4, "First Published")
            sheetStats.update_cell(1, 5, "Visible")
            sheetStats.update_cell(1, 6, "URL")
            sheetStats.update_cell(1, 7, "Stats Last Refrehed")

            log ("Wrote " + str(vizCount) + " records.")

            # If a new user, send the welcome email.
            if processed == False:
                send_new_user_email(emailList[i], firstnameList[i], urlStats)
                newCount += 1

        else:
            log ("No records written.")


    # Send email to Ken, indicating the number of new subscribers.
    msg = str(newCount) + " new subscribers have been added."
    subject = "Tableau Public Stats Service - " + str(newCount) + " New Subscribers"
    phone_home (subject, msg)

# Labmda will always call the lambda handler function, so this will not get run unless you are running locally.
# This code will connect to AWS locally. This requires a credentials file in C:\Users\<Username>\.aws\
# For further details, see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
if __name__ == "__main__":
    log("Code is running locally..............................................................")
    context = []
    event = {"state": "DISABLED"}
    boto3.setup_default_session(region_name="us-east-2", profile_name="default")
    lambda_handler(event, context)