import sys

for line in sys.stdin:
	v1, v2, w = line.split('\t')
	v1 = int(v1)
	v2 = int(v2)
	w = float(w)
	print(v1, str(v2) + ' ' + str(w), sep='\t')
	print(v2, str(v1) + ' ' + str(w), sep='\t')