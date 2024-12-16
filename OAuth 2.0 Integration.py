from flask import Flask, redirect, request, session, url_for
import requests

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Needed for session management

# GitHub OAuth credentials
CLIENT_ID = 'Ov23liPx5PfwnzbihJgn'
CLIENT_SECRET = 'b4b4f009221e07dfe8d08b88d49850eade5c867c'
REDIRECT_URI = 'http://localhost:5000/github_login/github/authorized'

# GitHub OAuth URLs
GITHUB_AUTH_URL = 'https://github.com/login/oauth/authorize'
GITHUB_TOKEN_URL = 'https://github.com/login/oauth/access_token'
GITHUB_API_BASE_URL = 'https://api.github.com'

@app.route('/')
def home():
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Login with GitHub</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                background-color: #f5f5f5;
                margin: 0;
            }
            .container {
                text-align: center;
            }
            h1 {
                margin-bottom: 20px;
                color: #333;
            }
            .login-button {
                background-color: #24292e;
                color: white;
                border: none;
                padding: 15px 25px;
                font-size: 18px;
                cursor: pointer;
                border-radius: 5px;
                text-decoration: none;
                display: inline-block;
            }
            .login-button:hover {
                background-color: #444d56;
            }
            .message {
                margin-top: 20px;
                font-size: 16px;
                color: #555;
            }
        </style>
        <script>
            function showWelcomeMessage() {
                document.getElementById('welcome-message').style.display = 'block';
            }
        </script>
    </head>
    <body onload="showWelcomeMessage()">
        <div class="container">
            <h1 id="welcome-message" style="display:none;">Welcome! Please log in with GitHub</h1>
            <a href="/login" class="login-button">Log in with GitHub</a>
            <p class="message">Click the button above to start the login process.</p>
        </div>
    </body>
    </html>
    '''

@app.route('/login')
def login():
    # Redirect to GitHub for authorization
    params = {
        'client_id': CLIENT_ID,
        'redirect_uri': REDIRECT_URI,
        'scope': 'repo user',
        'state': 'random_state_string'
    }
    authorization_url = f"{GITHUB_AUTH_URL}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
    return redirect(authorization_url)

@app.route('/github_login/github/authorized')
def callback():
    code = request.args.get('code')
    if code:
        # Exchange authorization code for access token
        data = {
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'code': code,
            'redirect_uri': REDIRECT_URI
        }
        headers = {
            'Accept': 'application/json'
        }
        response = requests.post(GITHUB_TOKEN_URL, data=data, headers=headers)
        if response.status_code == 200:
            client_credentials = response.json()
            session['access_token'] = client_credentials['access_token']
            return redirect(url_for('profile'))
        else:
            return "Failed to get the access token.", 400
    else:
        return "Authorization code not found.", 400

@app.route('/profile')
def profile():
    access_token = session.get('access_token')
    if access_token:
        headers = {
            'Authorization': f"token {access_token}"
        }
        response = requests.get(f"{GITHUB_API_BASE_URL}/user", headers=headers)
        if response.status_code == 200:
            user_info = response.json()
            return f"Logged in as: {user_info['login']}"
        else:
            return "Failed to fetch user info.", 400
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(port=5000)
