# OTX Aviation Threat Intelligence
A Python 3 application to collect a locally accessible storage of OTX pulses from AlienVault.
Objective: Create a locally accessible OTX pulse database for all and relivent OTX pulses.
## Overview
- Collects all OTX pulses from creation date
- Inserts them into an SQLite Table
- Collects all Aviation specific OTX pulses
- Inserts them into an SQLite Table
- Updates tables if data exists in tables
## Usage
`python otxmain.py <OTX API Key>`