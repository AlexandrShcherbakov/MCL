import sys

last_vertex = -1
edges = []

for line in sys.stdin:
	v, attr = line.split('\t')
	v = int(v)
	v1, w = attr.split(' ')
	v1 = int(v1)
	w = float(w)
	if last_vertex != v:
		if last_vertex != -1:
			print([last_vertex, 0, sorted(edges)])
		last_vertex = v;
		edges = []
	edges.append([v1, w])
print([last_vertex, 0, sorted(edges)])