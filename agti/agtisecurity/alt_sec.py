import secrets
import zlib
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

backend = default_backend()
iterations = 100_000

def _derive_key(password: bytes, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=iterations,
        backend=backend
    )
    return b64e(kdf.derive(password))

def password_encrypt(message: bytes, password: str) -> bytes:
    salt = secrets.token_bytes(8)
    key = _derive_key(password.encode(), salt)
    compressed = zlib.compress(message)
    ciphertext = Fernet(key).encrypt(compressed)
    return b64e(salt + b64d(ciphertext))

def password_decrypt(token: bytes, password: str) -> bytes:
    decoded = b64d(token)
    salt, ct = decoded[:8], b64e(decoded[8:])
    key = _derive_key(password.encode(), salt)
    return zlib.decompress(Fernet(key).decrypt(ct))

# Example usage:
encrypted = password_encrypt(message=b'life is too short', password='I dont want to go')
print(encrypted)

decrypted = password_decrypt(encrypted, password='I dont want to go')
print(decrypted)
