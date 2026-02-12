from cryptography.fernet import Fernet

# Generate a secure, random Fernet key
fernet_key = Fernet.generate_key()

# Decode from bytes to string for use in .env files
print(fernet_key.decode())
