from tornado import httpclient

http_client = httpclient.HTTPClient()
try:
    response = http_client.fetch('http://localhost:8888')
    print(response.body)
except httpclient.HTTPError as e:
    # HTTPError is raised for non-200 responses; the response
    # can be found in e.response.
    print("Error: " + str(e))
except Exception as e:
    # Other errors are possible, such as IOError.
    print("Error: " + str(e))
http_client.close()