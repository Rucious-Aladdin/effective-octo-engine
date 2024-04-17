import os

class filereader:
    def __init__(self, args):
        self.args = args
        self.filepath = os.path.join(args.filepath, args.stock_filename)
        self.cur_codes = []
        self.cur_counts = []
    
    def read(self):
        with open(self.filepath, "r", encoding="UTF8") as fp:
            lines = fp.readlines()
            lines = [line.rstrip("\n") for line in lines]
            for line in lines:
                code, count = line.split()
                self.cur_codes.append(code)
                self.cur_counts.append(int(count))
        return self.cur_codes, self.cur_counts

    def get_codes(self):
        return self.cur_codes

    def get_amounts(self):
        return self.cur_counts
    
    def get_all(self):
        return self.cur_codes, self.cur_counts