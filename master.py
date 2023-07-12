import grpc
import MapReduce_pb2
import MapReduce_pb2_grpc
import re
import time

input_path = "input_files/"

file1 = input_path + "input1.txt"
file2 = input_path + "input2.txt"
file3 = input_path + "input3.txt"

files = [file1, file2, file3]
maps = ["50052", "50053", "50054"]

for file in files:
    with open(file, "r") as f:
        data = f.read()

    s = re.split(" |\n", data)
    print(s)
    split_size = len(s) // 3
    Splits = [s[i:i + split_size] for i in range(0, len(s), split_size)]
    print(Splits)
    for chunk, map in zip(Splits, maps):
        channel = grpc.insecure_channel('localhost:{0}'.format(map))
        server_stub = MapReduce_pb2_grpc.mapStub(channel)
        request = MapReduce_pb2.input_split()
        request.input.extend(chunk)
        response = server_stub.inputSplits(request)
        print(response.response)

time.sleep(15)

reducers = ["50055", "50056"]
for reducer in reducers:
    channel = grpc.insecure_channel('localhost:'+reducer)
    server_stub = MapReduce_pb2_grpc.mapStub(channel)
    request = MapReduce_pb2.input_split(input=["map1", "map2", "map3"])
    response = server_stub.reducer_inputs(request)
    print(response.response)
