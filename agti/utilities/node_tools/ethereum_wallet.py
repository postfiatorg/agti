from web3 import Web3, Account
import secrets


class EthereumWallet:
    # USDT Contract Details
    USDT_CONTRACT_ADDRESS = '0xdAC17F958D2ee523a2206206994597C13D831ec7'
    USDT_ABI = [
        {
            "constant": False,
            "inputs": [
                {"name": "_to", "type": "address"},
                {"name": "_value", "type": "uint256"}
            ],
            "name": "transfer",
            "outputs": [{"name": "", "type": "bool"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function",
        },
    ]

    def __init__(self, infura_project_id, private_key=None):
        # Connect to Ethereum network using Infura
        self.w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{infura_project_id}"))

        # Generate or set the private key
        if private_key:
            self.private_key = private_key
        else:
            self.private_key = "0x" + secrets.token_hex(32)

        # Create an account from the private key
        self.account = Account.from_key(self.private_key)
        self.address = self.account.address

        print(f"Wallet Address: {self.address}")
        print(f"Private Key: {self.private_key}")

    def get_eth_balance(self):
        # Get Ether balance
        balance = self.w3.eth.get_balance(self.address)
        return self.w3.from_wei(balance, 'ether')

    def get_usdt_balance(self):
        # USDT Contract instance
        usdt_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(self.USDT_CONTRACT_ADDRESS), 
            abi=self.USDT_ABI
        )
        # Get USDT balance
        balance = usdt_contract.functions.balanceOf(self.address).call()
        return balance / (10 ** 6)  # Adjust for 6 decimal places

    def send_usdt(self, recipient_address, amount, gas_limit=100000):
        # Create USDT Contract instance
        usdt_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(self.USDT_CONTRACT_ADDRESS), 
            abi=self.USDT_ABI
        )
        
        # Convert recipient address to checksum address
        recipient = self.w3.to_checksum_address(recipient_address)
        
        # Prepare the transaction
        nonce = self.w3.eth.get_transaction_count(self.address)
        gas_price = self.w3.eth.gas_price
        amount_in_wei = int(amount * (10 ** 6))  # USDT has 6 decimals
        
        transaction = usdt_contract.functions.transfer(
            recipient, amount_in_wei
        ).build_transaction({
            'chainId': 1,  # Mainnet
            'gas': gas_limit,
            'gasPrice': gas_price,
            'nonce': nonce,
            'from': self.address,
        })

        # Sign the transaction
        signed_txn = self.w3.eth.account.sign_transaction(transaction, self.private_key)

        # Send the transaction
        txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
        print(f"Transaction hash: {txn_hash.hex()}")

        # Wait for receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)
        return txn_receipt


