from util import cal_edit_distance, stockprice

import pickle
import pandas as pd
import os
from tabulate import tabulate
from tqdm import tqdm


class dbreader:
    def __init__(self, args, code_into_name, name_into_code, raw_df, stockfilereader):
        self.args = args
        self.code_into_name = code_into_name
        self.name_into_code = name_into_code
        self.stockfilereader = stockfilereader
        self.target_df = self.make_target_df(raw_df)
        
        self.count_dict = None
        self.sell_df = None
        self.sell_list = None
        self.buy_df = None
        self.buy_list = None
        self.inter_list = None
        
        self.final_df = None
    
    @classmethod
    def load_dictionary_and_df(cls, args, stockfilereader):
        with open(os.path.join(args.filepath, args.dict_filename), 'rb') as fr:
            code_into_name = pickle.load(fr)
        name_into_code = {}
        for code, name in code_into_name.items():
            name_into_code[name] = code
        raw_df = pd.read_csv(os.path.join(args.filepath, args.quant_filename), encoding="cp949", sep=",")
        return cls(args, code_into_name, name_into_code, raw_df, stockfilereader)
    
    def fit(self):
        self.make_partition()
        self.make_sell_df()
        self.make_intersection_df()
        self.make_buy_df()
        self.concatenate_df()
        self.formatting()
        self.resorting()
        return
    
    def make_target_df(self, df):
        codes = df["code"].tolist()[:self.args.stock_number]
        names = df["company_name"].tolist()[:self.args.stock_number]
        prices_criterion = df["price"].tolist()[:self.args.stock_number]
        prices_current = []
        
        print(" %s 파일을 참고해 주가정보를 수집합니다.(20분 지연시세)" % str(self.args.quant_filename))
        for code in tqdm(codes):
            Pricedata = stockprice(code[1:], 1)
            prices_current.append(Pricedata[-1])
            
        info_df = pd.DataFrame({
            "종목코드": codes,
            "회사명": names,
            "평가당시금액(원)": prices_criterion,
            "현재가(원)": prices_current
        })
        info_df["평가금액변동률"] = round((info_df["현재가(원)"] - info_df["평가당시금액(원)"]) / info_df["평가당시금액(원)"] * 100, 2)
        info_df["평가순위"] = df["total_rank"]
        return info_df
    
    def make_partition(self):
        target_codes = set(self.target_df["종목코드"].tolist())
        cur_codes = self.stockfilereader.cur_codes
        cur_counts = self.stockfilereader.cur_counts
        if cur_codes:
            if not (ord("0") <= ord(cur_codes[0][0]) <= ord("9")):
                temp = []
                for code in cur_codes:
                    temp.append(self.name_to_code(code))
                cur_codes = temp
            else:
                cur_codes = ["A" + code for code in cur_codes]
        self.count_dict = {k:cur_counts[i] for i, k in enumerate(cur_codes)}
        cur_codes = set(cur_codes)
        self.sell_list = list(cur_codes - target_codes)
        self.buy_list = list(target_codes - cur_codes)
        self.inter_list = list(target_codes & cur_codes)
    
    def make_sell_df(self):
        sell_list = self.sell_list
        count_dict = self.count_dict
        sell_name_list = [self.code_into_name[code] for code in sell_list]
        
        prices_current = []
        if self.sell_list:
            print(" %s 파일을 참고해 팔아야할 주식정보를 수집합니다.(20분 지연시세)" % str(self.args.stock_filename))
            for code in tqdm(sell_list):
                Pricedata = stockprice(code[1:], 1)
                prices_current.append(Pricedata[-1])
            
        sell_df = pd.DataFrame({
            "종목코드": sell_list,
            "회사명": sell_name_list,
            "현재가(원)": prices_current,
            "보유수량": [count_dict[k] for k in sell_list],
            "거래예정수량": [-count_dict[k] for k in sell_list]
        })
        sell_df["리밸런싱"] = "전량매도"
        sell_df["거래예정금액(원)"] = -sell_df["현재가(원)"] * sell_df["보유수량"]
        self.sell_df = sell_df.sort_values(by=["회사명"], axis=0)
        self.sell_df = self.sell_df.reset_index(drop=True)

    def make_intersection_df(self):
        inter_list = self.inter_list
        inter_count = [self.count_dict[k] for k in inter_list]
        inter_df = pd.DataFrame({
            "종목코드": inter_list,
            "보유수량": inter_count
        })
        indi_amount = self.args.invest_amount / self.args.stock_number
        inter_df = pd.merge(left=self.target_df, right=inter_df, how="inner", on="종목코드")
        inter_df["Cur_Amount"] = inter_df["현재가(원)"] * inter_df["보유수량"]
        inter_df["거래예정금액(원)"] = indi_amount - inter_df["Cur_Amount"]
        inter_df["거래예정수량"] = inter_df["거래예정금액(원)"] // inter_df["현재가(원)"]
        
        def what_action(x):
            if x == 0:
                return "유지"
            elif x < 0:
                return "매도"
            else:
                return "매수"
        def what_rank(x):
            if x == "유지":
                return 1
            elif x == "매도":
                return 0
            else:
                return 2
        inter_df["리밸런싱"] = "유지"
        inter_df["리밸런싱"] = inter_df["거래예정수량"].apply(what_action)
        inter_df["예상이익금액(원)"] = (inter_df["평가당시금액(원)"] - inter_df["현재가(원)"]) * inter_df["거래예정수량"]
        inter_df["for_sort"] = 0
        inter_df["for_sort"] = inter_df["리밸런싱"].apply(what_rank)
        if self.args.sortbyrank:
            inter_df = inter_df.sort_values(by=["for_sort", "평가순위", "회사명"])
        else:
            inter_df = inter_df.sort_values(by=["for_sort", "평가금액변동률", "회사명"])
        inter_df = inter_df.drop(["for_sort"], axis=1)
        self.inter_df = inter_df.reset_index(drop=True)
    
    def make_buy_df(self):
        #"종목코드", "회사명", "평가당시금액(원)", "현재가(원)", "평가금액변동률", "평가순위"
        buy_list = self.buy_list
        buy_df = pd.DataFrame({
            "종목코드": buy_list
        })
        buy_df = pd.merge(left=self.target_df, right=buy_df, how="inner", on="종목코드")
        indi_amount = self.args.invest_amount / self.args.stock_number
        buy_df["리밸런싱"] = "전량매수"
        buy_df["거래예정금액(원)"] = indi_amount
        buy_df["거래예정수량"] = indi_amount // buy_df["현재가(원)"]
        buy_df["예상이익금액(원)"] = (buy_df["평가당시금액(원)"] - buy_df["현재가(원)"]) * buy_df["거래예정수량"]
        buy_df["보유수량"] = 0
        if self.args.sortbyrank:
            buy_df = buy_df.sort_values(by=["평가순위", "평가금액변동률"])
        else:
            buy_df = buy_df.sort_values(by=["평가금액변동률", "회사명"])
        self.buy_df = buy_df.reset_index(drop=True)
    
    def concatenate_df(self):
        temp = pd.concat([self.inter_df, self.buy_df], join="outer")
        self.final_df = pd.concat([self.sell_df, temp], join="outer")
        self.final_df = self.final_df[[
            "종목코드", "회사명", "평가당시금액(원)", "현재가(원)", "거래예정수량", \
            "리밸런싱", "평가금액변동률", "예상이익금액(원)", "거래예정금액(원)", \
            "보유수량","평가순위", "Cur_Amount"
        ]]
        self.vis_df = self.final_df.drop(["평가당시금액(원)", "현재가(원)", "Cur_Amount"], axis=1)
        self.final_df = self.final_df.reset_index(drop=True)
        self.vis_df = self.vis_df.reset_index(drop=True)
            
    def formatting(self):
        def format_money(x):
            try:
                return "{:,}".format(int(x))
            except:
                return x
        
        final_list = ["평가당시금액(원)", "현재가(원)", "거래예정수량", "예상이익금액(원)", "거래예정금액(원)", \
            "보유수량", "Cur_Amount"
            ]
        vis_list = ["거래예정수량", "예상이익금액(원)", "거래예정금액(원)", "보유수량"]
        for x in final_list:
            self.final_df[x] = self.final_df[x].apply(pd.to_numeric, errors='coerce').apply(format_money)
        for x in vis_list:
            self.vis_df[x] = self.vis_df[x].apply(pd.to_numeric, errors='coerce').apply(format_money)
   
    def resorting(self):
        if self.args.sortbyrank:
            #열정렬
            final_columns = self.final_df.columns.tolist()
            self.final_df = self.final_df[final_columns[:2] + [final_columns[-1]] + final_columns[2:-1]]
            vis_columns = self.vis_df.columns.tolist()
            self.vis_df = self.vis_df[vis_columns[:2] + [vis_columns[-1]] + vis_columns[2:-1]]

    def code_to_name(self, code):
        try:
            return self.code_into_name[code]
        except:
            return "KeyError"

    def name_to_code(self, name):
        try:
            return self.name_into_code[name]
        except:
            return "KeyError"
    
    def print_df(self):
        try:
            print(tabulate(self.vis_df, showindex=True, headers='keys', tablefmt='psql', stralign='right'))
        except:
            print("현재 dataframe을 출력할 수 없습니다.")
            print("%s 경로의 file을 참고하세요." % str(self.args.savepath))
    
    def save_df(self):
        filename1 = "detailed_transcription.csv"
        filename2 = "summary_transcription.csv"
        self.final_df.to_csv(os.path.join(self.args.savepath, filename1), index=False, encoding="cp949")
        if self.args.vis_df:
            self.vis_df.to_csv(os.path.join(self.args.savepath, filename2), index=False, encoding="cp949")
        