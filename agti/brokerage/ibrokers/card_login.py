import json
class IBLoginCard:
    def __init__(self,pw_map):
        self.pw_map = pw_map
    def output_card_login(self,req_1=52,req_2=160):
        request_json = json.loads(self.pw_map['ib_login_string'])
        op1=request_json[str(int(req_1))]
        op2=request_json[str(int(req_2))]
        output=op1+op2
        print(output)
        return output