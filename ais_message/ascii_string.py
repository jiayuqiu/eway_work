# coding:utf-8

class AISAnalysis(object):
    def python_substring(self, input_string, str_index, string_length):
        """
        c#中的Substring功能
        :param str_index: 开始的索引
        :param string_length: 需要截取字符串的长度
        :return: 需要的字符串
        """
        return input_string[str_index:(str_index + string_length)]
    
    def str_to_ascii(self, msg_string):
        """
        将ais报文转换为ASCII码
        :param msg_string: ais报文
        :return: ais报文的ascii码
        """
        ascii_string = ""
        for letter in msg_string:
            if letter == "0":
                ascii_string += "000000"
            
            elif letter == "1":
                ascii_string += "000001"
            
            elif letter == "2":
                ascii_string += "000010"
            
            elif letter == "3":
                ascii_string += "000011"
            
            elif letter == "4":
                ascii_string += "000100"
            
            elif letter == "5":
                ascii_string += "000101"
            
            elif letter == "6":
                ascii_string += "000110"
            
            elif letter == "7":
                ascii_string += "000111"
            
            elif letter == "8":
                ascii_string += "001000"
            
            elif letter == "9":
                ascii_string += "001001"
            
            elif letter == ":":
                ascii_string += "001010"
            
            elif letter == "":
                ascii_string += "001011"
            
            elif letter == "<":
                ascii_string += "001100"
            
            elif letter == "=":
                ascii_string += "001101"
            
            elif letter == ">":
                ascii_string += "001110"
            
            elif letter == "?":
                ascii_string += "001111"
            
            elif letter == "@":
                ascii_string += "010000"
            
            elif letter == "A":
                ascii_string += "010001"
            
            elif letter == "B":
                ascii_string += "010010"
            
            elif letter == "C":
                ascii_string += "010011"
            
            elif letter == "D":
                ascii_string += "010100"
            
            elif letter == "E":
                ascii_string += "010101"
            
            elif letter == "F":
                ascii_string += "010110"
            
            elif letter == "G":
                ascii_string += "010111"
            
            elif letter == "H":
                ascii_string += "011000"
            
            elif letter == "I":
                ascii_string += "011001"
            
            elif letter == "J":
                ascii_string += "011010"
            
            elif letter == "K":
                ascii_string += "011011"
            
            elif letter == "L":
                ascii_string += "011100"
            
            elif letter == "M":
                ascii_string += "011101"
            
            elif letter == "N":
                ascii_string += "011110"
            
            elif letter == "O":
                ascii_string += "011111"
            
            elif letter == "P":
                ascii_string += "100000"
            
            elif letter == "Q":
                ascii_string += "100001"
            
            elif letter == "R":
                ascii_string += "100010"
            
            elif letter == "S":
                ascii_string += "100011"
            
            elif letter == "T":
                ascii_string += "100100"
            
            elif letter == "U":
                ascii_string += "100101"
            
            elif letter == "V":
                ascii_string += "100110"
            
            elif letter == "W":
                ascii_string += "100111"
            
            elif letter == "`":
                ascii_string += "101000"
            
            elif letter == "a":
                ascii_string += "101001"
            
            elif letter == "b":
                ascii_string += "101010"
            
            elif letter == "c":
                ascii_string += "101011"
            
            elif letter == "d":
                ascii_string += "101100"
            
            elif letter == "e":
                ascii_string += "101101"
            
            elif letter == "f":
                ascii_string += "101110"
            
            elif letter == "g":
                ascii_string += "101111"
            
            elif letter == "h":
                ascii_string += "110000"
            
            elif letter == "i":
                ascii_string += "110001"
            
            elif letter == "j":
                ascii_string += "110010"
            
            elif letter == "k":
                ascii_string += "110011"
            
            elif letter == "l":
                ascii_string += "110100"
            
            elif letter == "m":
                ascii_string += "110101"
            
            elif letter == "n":
                ascii_string += "110110"
            
            elif letter == "o":
                ascii_string += "110111"
            
            elif letter == "p":
                ascii_string += "111000"
            
            elif letter == "q":
                ascii_string += "111001"
            
            elif letter == "r":
                ascii_string += "111010"
            
            elif letter == "s":
                ascii_string += "111011"
            
            elif letter == "t":
                ascii_string += "111100"
            
            elif letter == "u":
                ascii_string += "111101"
            
            elif letter == "v":
                ascii_string += "111110"
            
            elif letter == "w":
                ascii_string += "111111"
        
        return ascii_string
    
    def ascii_to_str(self, msg_string):
        """
        将二进制转换为字符串
        :param msg_string: 二进制字符串
        :return: 字符串
        """
        string_str = ""
        index = 0
        while index < len(msg_string):
            if msg_string[index:(index + 6)] == "000000":
                string_str += "@"
                break
            if msg_string[index:(index + 6)] == "000001":
                string_str += "A"
                break
            if msg_string[index:(index + 6)] == "000010":
                string_str += "B"
                break
            if msg_string[index:(index + 6)] == "000011":
                string_str += "C"
                break
            if msg_string[index:(index + 6)] == "000100":
                string_str += "D"
                break
            if msg_string[index:(index + 6)] == "000101":
                string_str += "E"
                break
            if msg_string[index:(index + 6)] == "000110":
                string_str += "F"
                break
            if msg_string[index:(index + 6)] == "000111":
                string_str += "G"
                break
            if msg_string[index:(index + 6)] == "001000":
                string_str += "H"
                break
            if msg_string[index:(index + 6)] == "001001":
                string_str += "I"
                break
            if msg_string[index:(index + 6)] == "001010":
                string_str += "J"
                break
            if msg_string[index:(index + 6)] == "001011":
                string_str += "K"
                break
            if msg_string[index:(index + 6)] == "001100":
                string_str += "L"
                break
            if msg_string[index:(index + 6)] == "001101":
                string_str += "M"
                break
            if msg_string[index:(index + 6)] == "001110":
                string_str += "N"
                break
            if msg_string[index:(index + 6)] == "001111":
                string_str += "O"
                break
            if msg_string[index:(index + 6)] == "010000":
                string_str += "P"
                break
            if msg_string[index:(index + 6)] == "010001":
                string_str += "Q"
                break
            if msg_string[index:(index + 6)] == "010010":
                string_str += "R"
                break
            if msg_string[index:(index + 6)] == "010011":
                string_str += "S"
                break
            if msg_string[index:(index + 6)] == "010100":
                string_str += "T"
                break
            if msg_string[index:(index + 6)] == "010101":
                string_str += "U"
                break
            if msg_string[index:(index + 6)] == "010110":
                string_str += "V"
                break
            if msg_string[index:(index + 6)] == "010111":
                string_str += "W"
                break
            if msg_string[index:(index + 6)] == "011000":
                string_str += "X"
                break
            if msg_string[index:(index + 6)] == "011001":
                string_str += "Y"
                break
            if msg_string[index:(index + 6)] == "011010":
                string_str += "Z"
                break
            if msg_string[index:(index + 6)] == "011011":
                string_str += "["
                break
            if msg_string[index:(index + 6)] == "011100":
                string_str += "\\"
                break
            if msg_string[index:(index + 6)] == "011101":
                string_str += "]"
                break
            if msg_string[index:(index + 6)] == "011110":
                string_str += "^"
                break
            if msg_string[index:(index + 6)] == "011111":
                string_str += "_"
                break
            if msg_string[index:(index + 6)] == "100000":
                string_str += " "
                break
            if msg_string[index:(index + 6)] == "100001":
                string_str += "!"
                break
            if msg_string[index:(index + 6)] == "100010":
                string_str += "\""
                break
            if msg_string[index:(index + 6)] == "100011":
                string_str += "#"
                break
            if msg_string[index:(index + 6)] == "100100":
                string_str += "$"
                break
            if msg_string[index:(index + 6)] == "100101":
                string_str += "%"
                break
            if msg_string[index:(index + 6)] == "100110":
                string_str += "&"
                break
            if msg_string[index:(index + 6)] == "100111":
                string_str += "'"
                break
            if msg_string[index:(index + 6)] == "101000":
                string_str += "("
                break
            if msg_string[index:(index + 6)] == "101001":
                string_str += ")"
                break
            if msg_string[index:(index + 6)] == "101010":
                string_str += "*"
                break
            if msg_string[index:(index + 6)] == "101011":
                string_str += "+"
                break
            if msg_string[index:(index + 6)] == "101100":
                string_str += ","
                break
            if msg_string[index:(index + 6)] == "101101":
                string_str += "-"
                break
            if msg_string[index:(index + 6)] == "101110":
                string_str += "."
                break
            if msg_string[index:(index + 6)] == "101111":
                string_str += "/"
                break
            if msg_string[index:(index + 6)] == "110000":
                string_str += "0"
                break
            if msg_string[index:(index + 6)] == "110001":
                string_str += "1"
                break
            if msg_string[index:(index + 6)] == "110010":
                string_str += "2"
                break
            if msg_string[index:(index + 6)] == "110011":
                string_str += "3"
                break
            if msg_string[index:(index + 6)] == "110100":
                string_str += "4"
                break
            if msg_string[index:(index + 6)] == "110101":
                string_str += "5"
                break
            if msg_string[index:(index + 6)] == "110110":
                string_str += "6"
                break
            if msg_string[index:(index + 6)] == "110111":
                string_str += "7"
                break
            if msg_string[index:(index + 6)] == "111000":
                string_str += "8"
                break
            if msg_string[index:(index + 6)] == "111001":
                string_str += "9"
                break
            if msg_string[index:(index + 6)] == "111010":
                string_str += ":"
                break
            if msg_string[index:(index + 6)] == "111011":
                string_str += ""
                break
            if msg_string[index:(index + 6)] == "111100":
                string_str += "<"
                break
            if msg_string[index:(index + 6)] == "111101":
                string_str += "="
                break
            if msg_string[index:(index + 6)] == "111110":
                string_str += ">"
                break
            if msg_string[index:(index + 6)] == "111111":
                string_str += "?"
                break
        return string_str
