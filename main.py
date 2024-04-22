"""
FlightClient.py

A simple arrow flight test client

"""

import pyarrow.flight as flight
import pandas as pd

if __name__ == "__main__":
    location = flight.Location.for_grpc_tcp("localhost", 8002)
    client = flight.FlightClient(location)

    call_options = flight.FlightCallOptions(
        headers=[(b"authorization", b"Basic YWRtaW46YWRtaW4=")]
    )
    ticket_data = b'{"query": "select count(*) from teststream", "startTime": "10days", "endTime": "now"}'
    reader = client.do_get(flight.Ticket(ticket_data), options=call_options)
    data = reader.read_all()
    df = data.to_pandas()
    json_data = df.to_json(orient="records")
    print(json_data)
