# DEVNOTES

Use Wireshark to show a live decoded view of the dialogue between PostgreSQL and Muutos:

```bash
# Assuming a no-TLS PostgreSQL instance listening on localhost:5437.
sudo tshark -i lo0 -f 'tcp port 5437' -Y pgsql -T fields -e frame.time_relative -e tcp.srcport -e pgsql.type -e pgsql.query -d tcp.port==5437,pgsql
```
