import requests
from requests_oauthlib import OAuth1
from typing import Dict, Optional
import logging

class TwitterBot:
    """A Twitter/X bot that can post tweets and reply to high-follower accounts."""
    
    BASE_URL = "https://api.twitter.com/2"  # Changed from api.x.com to api.twitter.com
    
    def __init__(self, credentials: Dict[str, str]):
        """
        Initialize the TwitterBot with API credentials.
        
        Args:
            credentials: Dictionary containing 'consumer_key', 'consumer_secret',
                       'access_token', and 'access_token_secret'
        """
        self.auth = OAuth1(
            credentials['consumer_key'],
            credentials['consumer_secret'],
            credentials['access_token'],
            credentials['access_token_secret']
        )
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> requests.Response:
        """
        Make an authenticated request to the Twitter API.
        
        Args:
            method: HTTP method ('GET' or 'POST')
            endpoint: API endpoint to call
            data: Optional data payload for POST requests
            
        Returns:
            Response object from the API
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = requests.request(
                method=method,
                url=url,
                json=data,
                auth=self.auth,
                headers={"Content-Type": "application/json"}
            )
            
            # Log the complete response for debugging
            self.logger.info(f"Response Status: {response.status_code}")
            self.logger.info(f"Response Body: {response.text}")
            
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            self.logger.error(f"Response content: {e.response.text if hasattr(e, 'response') else 'No response content'}")
            raise
            
    def get_user_info(self, user_id: str) -> Dict:
        """
        Get information about a Twitter user.
        
        Args:
            user_id: Twitter user ID
            
        Returns:
            Dictionary containing user information
        """
        endpoint = f"users/{user_id}?user.fields=public_metrics"
        response = self._make_request("GET", endpoint)
        return response.json()
        
    def post_tweet(self, text: str) -> Dict:
        """
        Post a new tweet.
        
        Args:
            text: Content of the tweet
            
        Returns:
            Dictionary containing the response from Twitter
        """
        endpoint = "tweets"
        payload = {"text": text}
        
        response = self._make_request("POST", endpoint, payload)
        self.logger.info(f"Tweet posted successfully: {response.json()}")
        return response.json()
        
    def reply_to_tweet(self, tweet_id: str, text: str) -> Dict:
        """
        Reply to a tweet directly without follower count check.
        
        Args:
            tweet_id: ID of the tweet to reply to
            text: Content of the reply
            
        Returns:
            Dictionary containing the response from Twitter
        """
        endpoint = "tweets"
        payload = {
            "text": text,
            "reply": {
                "in_reply_to_tweet_id": tweet_id
            }
        }
        
        self.logger.info(f"Sending reply payload: {payload}")
        response = self._make_request("POST", endpoint, payload)
        self.logger.info(f"Reply attempt completed")
        return response.json()
""" 
# Example usage for a direct reply
if __name__ == "__main__":
    # Initialize credentials
    credentials = {
        'consumer_key': password_loader.pw_map['x_consumer_key'],
        'consumer_secret': password_loader.pw_map['x_consumer_secret'],
        'access_token': password_loader.pw_map['x_access_token'],
        'access_token_secret': password_loader.pw_map['x_access_token_secret']
    }
    
    # Create bot instance
    bot = TwitterBot(credentials)
    
    try:
        # Reply to the specific tweet
        tweet_id = "1850519741082829183"  # The tweet ID you provided
        reply_text = "Your reply message here"
        
        reply_response = bot.reply_to_tweet(tweet_id, reply_text)
        print(f"Reply response: {reply_response}")
            
    except Exception as e:
        print(f"Error occurred: {str(e)}")
"""