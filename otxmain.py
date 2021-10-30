import argparse
import otx

parser = argparse.ArgumentParser(description="Create and Maintain a local SQLite Database of OTX Pulses")
parser.add_argument("key", help="OTX API Key supplied from a valid OTX account")
args = parser.parse_args()


if __name__ == '__main__':
    app_dir = otx.ApplicationDirector(args.key)
    app_dir.update_alltables()
