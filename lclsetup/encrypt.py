import base64
import argparse

def encrypt_password(password):
    encoded_password = base64.b64encode(password.encode('utf-8'))
    return encoded_password.decode('utf-8')

def decrypt_password(encrypted_password):
    decoded_password = base64.b64decode(encrypted_password.encode('utf-8'))
    return decoded_password.decode('utf-8')


def main():
    password = None
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--decrypt', action='store_true', help='Decrypt the password')
    parser.add_argument('-e', '--encrypt', action='store_true', help='Encrypt the password')
    parser.add_argument('-p', '--password', type=str, help='The password to encrypt or decrypt')

    args = parser.parse_args()

    if args.decrypt:
        decrypted_password = decrypt_password(args.password)
        print(decrypted_password)
    elif args.encrypt:
        encrypted_password = encrypt_password(args.password)
        print(encrypted_password)
    else:
        print("Please specify either -d/--decrypt or -e/--encrypt option.")
    # Example usage
    if password:
        encrypted_password = encrypt_password(password)
        print(encrypted_password)

if __name__ == "__main__":
    main()

import base64
import argparse

def encrypt_password(password):
    encoded_password = base64.b64encode(password.encode('utf-8'))
    return encoded_password.decode('utf-8')

def decrypt_password(encrypted_password):
    decoded_password = base64.b64decode(encrypted_password.encode('utf-8'))
    return decoded_password.decode('utf-8')



def main():
    password = None
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--decrypt', action='store_true', help='Decrypt the password')
    parser.add_argument('-e', '--encrypt', action='store_true', help='Encrypt the password')
    parser.add_argument('-p', '--password', type=str, help='The password to encrypt or decrypt')

    args = parser.parse_args()

    if args.decrypt:
        decrypted_password = decrypt_password(args.password)
        print(decrypted_password)
    elif args.encrypt:
        encrypted_password = encrypt_password(args.password)
        print(encrypted_password)
    else:
        print("Please specify either -d/--decrypt or -e/--encrypt option.")
    # Example usage
    if password:
        encrypted_password = encrypt_password(password)
        print(encrypted_password)

if __name__ == "__main__":
    main()
