import requests
import logging
import argparse
import sys
import os
import subprocess
import platform

# Create logs directory structure if it doesn't exist
script_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(script_dir, 'logs')
messaging_logs_dir = os.path.join(logs_dir, 'messaging')
os.makedirs(messaging_logs_dir, exist_ok=True)

# Configure logging with timestamps for detailed output to both console and file
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler(os.path.join(messaging_logs_dir, 'message_log.txt'), mode='a', encoding='utf-8')  # File output, append mode
    ]
)
logger = logging.getLogger(__name__)

# Hardcode the webhook URL (replace with your actual webhook URL)
WEBHOOK_URL = ""

# Internal toggle to enable or disable sending the message
SEND_MESSAGE = True  # Set to False to disable sending and only print the message True

# Toggle to open a new console window when run with arguments
OPEN_CONSOLE = False  # Set to False to disable opening a new console window entirely

# Toggle to pause execution with "Press Enter to exit..." prompt
PAUSE_ON_EXIT = False  # Set to False to exit immediately without pausing

def safe_print(message):
    """Print messages safely, handling Unicode encoding issues"""
    try:
        print(message)
    except UnicodeEncodeError:
        # If Unicode fails, print a safe version
        safe_message = message.encode('ascii', errors='replace').decode('ascii')
        print(safe_message)

# Set up argument parser
parser = argparse.ArgumentParser(
    description='Send a message to Mattermost webhook. Enclose message in quotes if it contains special characters.'
)
parser.add_argument(
    'message',
    type=str,
    help='The message to send to Mattermost. Use quotes for special characters like quotes or spaces.'
)
parser.add_argument(
    '--no-send',
    action='store_true',
    help='Do not send the message, just print it to the console.'
)
parser.add_argument(
    '--no-console',
    action='store_true',
    help='Do not open a new console window.'
)
parser.add_argument(
    '--no-pause',
    action='store_true',
    help='Do not pause execution with "Press Enter to exit..." prompt.'
)
args = parser.parse_args()

# Determine if sending is enabled based on the toggle and the flag
should_send = SEND_MESSAGE and not args.no_send

# Determine if pausing is enabled based on the toggle and the flag
should_pause = PAUSE_ON_EXIT and not args.no_pause

# Check if the script should open a new console
if OPEN_CONSOLE and len(sys.argv) > 1 and not args.no_console:
    # Open a new console window to display logging information
    if platform.system() == "Windows":
        subprocess.Popen(["start", "cmd", "/k", "python", sys.argv[0]] + sys.argv[1:], shell=True)
    elif platform.system() == "Darwin" or platform.system() == "Linux":
        subprocess.Popen(["gnome-terminal", "--", "python3", sys.argv[0]] + sys.argv[1:])
    else:
        safe_print("Unsupported platform for opening a new console window.")
    sys.exit(0)

# Print initial script execution details with safe printing
safe_print("[TEST] testmattermostmsg.py started")
safe_print(f"[TEST] Command-line arguments received: {len(sys.argv[1:])} arguments")
safe_print(f"[TEST] Message length: {len(args.message)} characters")

# Indicate the mode of operation
if not should_send:
    safe_print("[MODE] No-send mode: message will not be sent, only printed.")
else:
    safe_print("[MODE] Send mode: attempting to send message to Mattermost.")

# Main logic
if not should_send:
    # Print the message and conditionally pause
    safe_print("--- Message to be sent ---")
    try:
        print(args.message)
    except UnicodeEncodeError:
        safe_print("Message contains Unicode characters that cannot be displayed in this console.")
        safe_print(f"Message length: {len(args.message)} characters")
    safe_print("--------------------------")
    safe_print("[TEST] testmattermostmsg.py execution completed (no-send mode)")
    if should_pause:
        input("Press Enter to exit...")
else:
    # Check if the webhook URL is set
    if not WEBHOOK_URL:
        safe_print("[ERROR] Webhook URL is not set.")
        safe_print("[TEST] testmattermostmsg.py execution completed with errors")
        if should_pause:
            input("Press Enter to exit...")
    else:
        # Attempt to send the message
        try:
            logger.debug("Sending POST request to Mattermost webhook...")
            safe_print("[TEST] Initiating POST request to Mattermost")
            response = requests.post(WEBHOOK_URL, json={"text": args.message})
            safe_print("[TEST] POST request sent")
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"Response text: {response.text}")
            safe_print(f"[TEST] Response status code: {response.status_code}")
            safe_print(f"[TEST] Response text: {response.text}")
            
            if response.status_code == 200:
                # Success: log, print, and exit
                logger.info("Message sent successfully.")
                safe_print("[TEST] Message sent successfully to Mattermost")
                safe_print("[TEST] testmattermostmsg.py execution completed successfully")
                if should_pause:
                    input("Press Enter to exit...")
            else:
                # Failure: log, print error and message, then conditionally pause
                logger.warning(f"Failed to send message. Status code: {response.status_code}")
                safe_print(f"[TEST] Failed to send message. Status code: {response.status_code}")
                safe_print("--- Message that was attempted to be sent ---")
                try:
                    print(args.message)
                except UnicodeEncodeError:
                    safe_print("Message contains Unicode characters that cannot be displayed.")
                safe_print("--------------------------------------------")
                safe_print("[TEST] testmattermostmsg.py execution completed with errors")
                if should_pause:
                    input("Press Enter to exit...")
        except requests.exceptions.RequestException as e:
            # Handle network-related errors
            logger.error(f"An error occurred: {e}")
            safe_print(f"[TEST] RequestException occurred: {str(e)}")
            safe_print("--- Message that was attempted to be sent ---")
            try:
                print(args.message)
            except UnicodeEncodeError:
                safe_print("Message contains Unicode characters that cannot be displayed.")
            safe_print("--------------------------------------------")
            safe_print("[TEST] testmattermostmsg.py execution completed with errors")
            if should_pause:
                input("Press Enter to exit...")
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Unexpected error: {e}")
            safe_print(f"[TEST] Unexpected error: {str(e)}")
            safe_print("--- Message that was attempted to be sent ---")
            try:
                print(args.message)
            except UnicodeEncodeError:
                safe_print("Message contains Unicode characters that cannot be displayed.")
            safe_print("--------------------------------------------")
            safe_print("[TEST] testmattermostmsg.py execution completed with errors")
            if should_pause:
                input("Press Enter to exit...")