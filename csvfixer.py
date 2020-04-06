import argparse
import re
import tempfile

argParser = argparse.ArgumentParser(description='Process IMC Famos Logs')

argParser.add_argument('input', metavar='i', nargs='+',
                    help='The Input Files')

argParser.add_argument('--output-prefix', help='The CSV Output File Prefix',
    default='out')

args = argParser.parse_args()

out_file_name = tempfile.mktemp(".csv", args.output_prefix) 

for file_name in args.input:

    print("File: '{0}' - '{1}'".format(file_name, out_file_name))

    with open(file_name, "rb") as input:
       with open(out_file_name, "wb") as output:
           while True:
        
               c = input.read(1)

               if not c:
                   print("End of File")
                   break;

               elif c == b'\xc9':
                   print("Found Byte - 0xc9")
                   output.write(b'e')
               elif c == b'\x00':
                   print("Found Byte - 0x00")
               elif c > b'\xff':
                   print("Invalid Character: ".format(hex(ord(c))))

               else:
                   output.write(c) 
        


