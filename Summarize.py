#  This code will loop through loop through the stats Google sheets then summarize them--one row per profile--in another Google Sheet.
#  Written by Ken Flerlage, January, 2023.

import sys
import json
import gspread
import datetime
import time
import boto3
from oauth2client.service_account import ServiceAccountCredentials
from botocore.exceptions import ClientError

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
    # Summarize the stats before closing

    if msg != '':
        log(msg)
    
    exit()

#------------------------------------------------------------------------------------------------------------------------------
# Main lambda handler
#------------------------------------------------------------------------------------------------------------------------------
def lambda_handler(event, context):
    # Summarize all of the stats.

    log("Summarizing the stats for all users.")

    # Get the Google Sheets credentials from S3
    s3 = boto3.client('s3')
    key = credsFile
    object = s3.get_object(Bucket=s3Bucket, Key=key)
    content = object['Body']
    creds = json.loads(content.read())

    # Read your Google API key from a local json file.
    scope =['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    gc = gspread.authorize(credentials) 

    # Read the sign-up and summary sheets.
    docProfiles = gc.open_by_url('https://docs.google.com/spreadsheets/d/' + worksheetID)
    sheetProfiles = docProfiles.worksheet("Form Responses 1")
    sheetSummary = docProfiles.worksheet("Summary")

    # Get columns from the profiles sheet.
    emailList = sheetProfiles.col_values(2)
    firstnameList = sheetProfiles.col_values(3)
    lastnameList = sheetProfiles.col_values(4)
    profileList = sheetProfiles.col_values(5)
    urlList = sheetProfiles.col_values(6)
    dateList = sheetProfiles.col_values(7)
    profileCount = len(emailList)-1

    matrix = {}
    refreshDate = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    for i in range(1, profileCount+1):
        log("Proessing profile " + str(i) + " of " + str(profileCount))
        
        try:
            # Open each sheet then summarize the stats.
            docStats = gc.open_by_url(urlList[i])
            sheetStats = docStats.get_worksheet(0)

            # Sum up each of the metrics
            views = sheetStats.col_values(10)
            viewsCount = 0
            for j in range(1, len(views)):
                viewsCount += int(views[j])

            favorites = sheetStats.col_values(11)
            favoritesCount = 0
            for j in range(1, len(favorites)):
                favoritesCount += int(favorites[j])

            # Followers and following are repeated so just get first row.
            followers = sheetStats.col_values(24)
            followersCount = int(followers[1])

            following = sheetStats.col_values(25)
            followingCount = int(following[1])

            # Sum up visable vizzes only.
            visable = sheetStats.col_values(8)
            vizCount = 0
            for j in range(1, len(visable)):
                if visable[j]=='TRUE':
                    vizCount+=1

            # Write to the matrix
            matrix[i, 0] = firstnameList[i]
            matrix[i, 1] = lastnameList[i]
            matrix[i, 2] = profileList[i]
            matrix[i, 3] = urlList[i]
            matrix[i, 4] = str(favoritesCount)
            matrix[i, 5] = str(viewsCount)
            matrix[i, 6] = str(followersCount)
            matrix[i, 7] = str(followingCount)
            matrix[i, 8] = str(vizCount)
            matrix[i, 9] = dateList[i]
            matrix[i,10] = refreshDate 

        except Exception as e:
            # Google API can be finicky. 
            # Use the existing values, log the error, pause before continuing.
            matrix[i, 0]  = sheetSummary.col_values(1)[i]
            matrix[i, 1]  = sheetSummary.col_values(2)[i]
            matrix[i, 2]  = sheetSummary.col_values(3)[i]
            matrix[i, 3]  = sheetSummary.col_values(4)[i]
            matrix[i, 4]  = sheetSummary.col_values(5)[i]
            matrix[i, 5]  = sheetSummary.col_values(6)[i]
            matrix[i, 6]  = sheetSummary.col_values(7)[i]
            matrix[i, 7]  = sheetSummary.col_values(8)[i]
            matrix[i, 8]  = sheetSummary.col_values(9)[i]
            matrix[i, 9]  = sheetSummary.col_values(10)[i]
            matrix[i,10]  = sheetSummary.col_values(11)[i]

            # Log the error.
            msg = "Error processing profile # " + str(i) + " (" + sheetSummary.col_values(1)[i] + " " + sheetSummary.col_values(2)[i] + "): " + str(sys.exc_info()[0]) + " - " + str(e) 
            log (msg)

            subject = "Tableau Public Stats Sumarization Error"
            phone_home (subject, msg)

            msg = "Pausing for 10 seconds..."
            log (msg)

            time.sleep(10)
            continue

    # Write the matrix array to the Summary Sheet.
    log("Writing summary stats to sheet.")
    rangeString = "A2:K" + str(profileCount+1)
    cell_list = sheetSummary.range(rangeString)

    row = 1
    column = 0

    for cell in cell_list: 
        cell.value = matrix[row,column]
        column += 1
        if (column > 10):
            column=0
            row += 1

    # Update in batch   
    sheetSummary.update_cells(cell_list)
       

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
