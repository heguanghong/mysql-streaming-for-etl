import sys
import base64

def encrpt( s, key):
        s = base64.encodestring(s)
        b = bytearray(str(s).encode("gbk"))
        n = len(b)
        c = bytearray(n*2)
        j = 0
        for i in range(0, n):
                b1 = b[i]
                b2 = b1 ^ key # b1 = b2^ key
                c1 = b2 % 17
                c2 = b2 // 17 # b2 = c2*16 + c1
                c1 = c1 + 65
                c2 = c2 + 65
                c[j] = c1
                c[j+1] = c2
                j = j+2
        return c.decode("gbk")

def decrpt(s,key):
        c = bytearray(str(s).encode("gbk"))
        n = len(c)
        if n % 2 != 0 :
                return ""
        n = n // 2
        b = bytearray(n)
        j = 0
        for i in range(0, n):
                c1 = c[j]
                c2 = c[j+1]
                j = j+2
                c1 = c1 - 65
                c2 = c2 - 65
                b2 = c2*17 + c1
                b1 = b2^ key
                b[i]= b1
        b = base64.decodestring(b)
        try:
                return b.decode("gbk")
        except:
                return "failed"
