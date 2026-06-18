import json
from google_auth_oauthlib.flow import InstalledAppFlow

# Make sure this matches the exact scope in your main script
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def main():
    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
    # This will cleanly open your local web browser
    creds = flow.run_local_server(port=0)
    
    print("\n" + "="*60)
    print("👉 COPY EVERYTHING BETWEEN THESE LINES FOR GDRIVE_TOKEN_JSON 👈")
    print("="*60 + "\n")
    print(creds.to_json())
    print("\n" + "="*60)

if __name__ == '__main__':
    main()