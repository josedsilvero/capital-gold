import sys
date_conversion = {'last_3d':3, 'last_7d':7, 'last_14d':14}
print(date_conversion)

if len(sys.argv)>1:
    print("\n".join(sys.argv))
else:
    print("No hay argumentos")