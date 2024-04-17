import file_reader
import dbreader
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--transaction", dest="transaction", action="store_true")
parser.add_argument("-i", "--invest_amount", dest="invest_amount", action="store")
parser.add_argument("-n", "--stock_number", dest="stock_number", action="store")
parser.add_argument("-p", "--filepath", dest="filepath", action="store")
parser.add_argument("-s", "--savepath", dest="savepath", action="store")
parser.add_argument("-rk", "--sortbyrank", dest="sortbyrank", action="store_true")
parser.add_argument("-sv", "--visdf", dest="vis_df", action="store_false")
args = parser.parse_args()

transaction_codes = []
if args.transaction: # 개발예정
    N = input("거래정지된 종목수를 입력하세요: ")
    for i in range(N):
        transaction_codes.append(input("종목코드(또는 종목이름) 입력: "))

if args.invest_amount is None:
    args.invest_amount = float(1e7)
else:
    args.invest_amount = float(args.invest_amount)

if args.stock_number is None:
    args.stock_number = 25
else:
    args.stock_number = int(args.stock_number)

if args.filepath is None:
    args.filepath = "data/"

if args.savepath is None:
    args.savepath = "result/"

args.quant_filename = "quant_final.csv"
args.stock_filename = "current.txt"
args.dict_filename = "code_into_name.pickle"

## 보유중인 종목을 저장하는 클래스
stockfilereader = file_reader.filereader(args)
stockfilereader.read()

## quant_final.csv를 참고하는 클래스
stock_reader = dbreader.dbreader.load_dictionary_and_df(args, stockfilereader)
stock_reader.fit()
stock_reader.print_df()
stock_reader.save_df()
#------------여기까지 잘동작------------------


