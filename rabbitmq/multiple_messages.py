import requests


if __name__ == "__main__":
    for _ in range(100000):
        response = requests.post(f"http://localhost:8000/send-message?message={_ * 3}")
