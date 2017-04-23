import requests.auth

class RedditAuth:

    CLIENT_ID = "Ba1ko5k74vJeow"
    CLIENT_SECRET = "8162uBJkcz2R6LNg4DFh0DSO-FI"
    USER_NAME = "lzc_test"
    PASSWORD = "test123456"
    AUTH_URL = "https://www.reddit.com/api/v1/access_token"

    def get_token(self):
        client_auth = requests.auth.HTTPBasicAuth(self.CLIENT_ID, self.CLIENT_SECRET)
        post_data = {"grant_type": "password", "username": self.USER_NAME, "password": self.PASSWORD}
        headers = {"User-Agent": "big_data_demo"}
        response = requests.post(self.AUTH_URL, auth=client_auth, data=post_data, headers=headers)
        return response.json()["access_token"]

if __name__ == "__main__":
    reddit_auth = RedditAuth()
    reddit_auth.get_token()
    print("ok")