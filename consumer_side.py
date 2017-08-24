import requests
import atexit
import unittest
from pact import Consumer, Provider


def user(user_name):
    uri = 'http://localhost:1235/users/' + user_name
    return requests.get(uri).json()

pact = Consumer('Consumer').has_pact_with(Provider('Provider'), host_name='localhost', port=1235,pact_dir='/home')
pact.start_service()
atexit.register(pact.stop_service)


class GetUserInfoContract(unittest.TestCase):
    def test_get_user(self):
        expected = {
            'username': 'UserA',
            'id': 123,
            'groups': ['Editors']
        }
        (
            pact.given("UserA exits and is not an administrator")
            .upon_receiving('a requests for UserA')
            .with_request('get', '/users/UserA')
            .will_respond_with(200, body=expected)
        )
        with pact:
            result = user('UserA')
            self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
