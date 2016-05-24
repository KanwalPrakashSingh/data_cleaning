
import math

"""
Assumption here is that all the quantities prices are of a single instrument, as there is not instrument id mentioned. 
Also wasnt much clear on what parameters have to be analyzed for the report, so for each row (transaction record) the program will print the total_buy,average_buy_price,total_sell,average_sell_price,net_position,settled_quantity,realised_profit in the order.
"""
class Pnl:
    total_buy = 0
    total_sell = 0
    average_buy_price = 0
    average_sell_price = 0
    net_position = 0
    realised_profit = 0
    settled_quantity = 0
    count = 0
    def __init__(self,quantity,price):
        self.quantity = quantity
        self.price = price
        if quantity > 0: #buy
            Pnl.average_buy_price = 1.0*((Pnl.average_buy_price*Pnl.total_buy) + (price*quantity)) / (Pnl.total_buy + quantity)
            Pnl.total_buy += quantity
        else : 
            Pnl.average_sell_price = 1.0*((Pnl.average_sell_price*Pnl.total_sell) + (price*quantity)) / (Pnl.total_sell + quantity)
            Pnl.total_sell += -1*quantity

        Pnl.net_position = Pnl.total_buy-Pnl.total_sell
        Pnl.settled_quantity = min(Pnl.total_buy,Pnl.total_sell)
        Pnl.realised_profit = Pnl.settled_quantity*(Pnl.average_buy_price - Pnl.average_sell_price)
        Pnl.count = Pnl.count + 1

    def convert_to_list(self):
        return [Pnl.total_buy,Pnl.average_buy_price,Pnl.total_sell,Pnl.average_sell_price,Pnl.net_position,Pnl.settled_quantity,Pnl.realised_profit]



def read_file(filename):
    pnl_list = [] #total_buy,average_buy_price,total_sell,average_sell_price,net_position,settled_quantity,realised_profit
    with open(filename) as infile:
        for line in infile:
            row = line.split(" ")
            if row[0] == "qty":
                continue #first line
            val1 = float(row[0])
            val2 = float(row[1])
            if math.isnan(val1) or math.isnan(val2):
                continue
            current_pnl = Pnl(val1,val2)
            pnl_list.append(current_pnl.convert_to_list())
    print "total_buy , average_buy_price , total_sell , average_sell_price , net_position , settled_quantity , realised_profit"
    for i in range(len(pnl_list)):
        print pnl_list[i]

read_file('trades.txt')

