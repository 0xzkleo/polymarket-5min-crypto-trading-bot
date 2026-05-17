"""scripts/redis_control.py — Toggle simulation/live mode via Redis without restarting the bot."""
import redis
import sys
import os
from dotenv import load_dotenv

load_dotenv()


def get_redis_client():
    try:
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 2)),
            decode_responses=True,
            socket_connect_timeout=5,
        )
        client.ping()
        return client
    except Exception as e:
        print(f"Redis connection failed: {e}")
        print("Make sure Redis is running: redis-server")
        return None


def get_current_mode(client) -> bool | None:
    try:
        mode = client.get("btc_trading:simulation_mode")
        return None if mode is None else (mode == "1")
    except Exception as e:
        print(f"Error reading mode: {e}")
        return None


def set_simulation_mode(client, simulation: bool) -> bool:
    try:
        client.set("btc_trading:simulation_mode", "1" if simulation else "0")
        print(f"Mode set to: {'SIMULATION' if simulation else 'LIVE TRADING'}")
        return True
    except Exception as e:
        print(f"Error setting mode: {e}")
        return False


def display_status(client) -> None:
    mode = get_current_mode(client)
    print("\n" + "=" * 60)
    print("BTC BOT — CURRENT STATUS")
    print("=" * 60)
    if mode is None:
        print("Status: Not set (using default from .env)")
    elif mode:
        print("Status: SIMULATION MODE")
        print("  No real trades will be placed")
    else:
        print("Status: LIVE TRADING MODE")
        print("  REAL MONEY AT RISK!")
    print("=" * 60 + "\n")


def main() -> None:
    print("\n" + "=" * 60)
    print("BTC BOT — SIMULATION MODE CONTROL")
    print("=" * 60)

    client = get_redis_client()
    if not client:
        return

    display_status(client)

    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command in ("sim", "simulation", "on"):
            set_simulation_mode(client, True)
            display_status(client)
        elif command in ("live", "off"):
            print("WARNING: Switching to LIVE TRADING mode!")
            confirm = input("Type 'yes' to confirm: ")
            if confirm.lower() == "yes":
                set_simulation_mode(client, False)
                display_status(client)
            else:
                print("Cancelled.")
        elif command in ("status", "check"):
            pass
        else:
            print(f"Unknown command: {command}")
            print("\nUsage:")
            print("  python scripts/redis_control.py sim    — Enable simulation mode")
            print("  python scripts/redis_control.py live   — Enable live trading")
            print("  python scripts/redis_control.py status — Show current status")
    else:
        print("Commands:")
        print("  1. Enable simulation mode")
        print("  2. Enable live trading")
        print("  3. Check status")
        print("  4. Exit")
        while True:
            try:
                choice = input("\nEnter choice (1-4): ").strip()
                if choice == "1":
                    set_simulation_mode(client, True)
                    display_status(client)
                elif choice == "2":
                    print("WARNING: This will enable LIVE TRADING!")
                    confirm = input("Type 'yes' to confirm: ")
                    if confirm.lower() == "yes":
                        set_simulation_mode(client, False)
                        display_status(client)
                    else:
                        print("Cancelled.")
                elif choice == "3":
                    display_status(client)
                elif choice == "4":
                    print("Goodbye!")
                    break
                else:
                    print("Invalid choice!")
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break


if __name__ == "__main__":
    main()
