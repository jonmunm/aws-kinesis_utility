import argparse
import asyncio
from sender import command as send 
from receiver import command as receive 

def main():   
    parser = argparse.ArgumentParser(description="CLI for Kinesis interactions")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Configuration for 'send' command updated to use --batches
    parser_send = subparsers.add_parser('send', help="Send records in parallel batches to a Kinesis Stream")
    parser_send.add_argument('--stream', required=True, type=str, help="Kinesis Stream name")
    parser_send.add_argument('--batches', required=True, type=int, default=5, help="Number of concurrent batches to send")

    # Configuration for 'receive' command
    parser_receive = subparsers.add_parser('receive', help="Receive records from a Kinesis Stream")
    parser_receive.add_argument('--stream', required=True, type=str, help="Kinesis Stream name")

    args = parser.parse_args()
    
    if args.command == 'send':
        asyncio.run(send(
            stream=args.stream,
            batches=args.batches
        ))
            
    elif args.command == 'receive':
        asyncio.run(receive(
            stream=args.stream
        ))
            
    else:
        parser.print_help() 

if __name__ == "__main__":
    main()