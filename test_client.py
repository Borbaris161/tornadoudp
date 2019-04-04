from asynclient import UDPClient,  UDPClientError

udp_client = UDPClient()
try:
    response = udp_client.fetch("udp://10.1.4.6:1111/controller?init")
    print(response)
except UDPClientError as e:
    # HTTPError is raised for non-200 responses; the response
    # can be found in e.response.
    print("Error: " + str(e))
except Exception as e:
    # Other errors are possible, such as IOError.
    print("Error: " + str(e))
udp_client.close()


# max_client = 1
#
# i = 0

# clients = [AsyncUDPClient() for client in range(max_client)]


# async def f():
#     http_client = AsyncUDPClient()
#     try:
#         response = await http_client.fetch("udp://10.1.4.6:1111/controller?init")
#         return response
#     except Exception as e:
#         print("Error: %s" % e)
#     else:
#         print(response.body)
# a = f()


# while i < max_client:
#    response = clients[i].fetch("udp://10.1.4.6:1111/controller?init")
#    print(response, 'response')
#    i += 1
#
# IOLoop.instance().start()