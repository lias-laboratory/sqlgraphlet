
import vertica_python as vp
import pandas as pd
import sys
import cStringIO


#----------------- Function to measure query running time ---------------#
def get_time(cur):
	time = 0
	cur.execute("SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds FROM current_session;")
	if cur.rowcount != 0:
		exe_time = cur.fetchall()
		for i,t in enumerate(exe_time):
			time = float(t[0])
	return time
	
#----------------- Drop Table -----------------------------------------#
def drop_table(table_name):
	sql_string="DROP TABLE IF EXISTS "+table_name+" CASCADE;"
	cur.execute(sql_string)
   
#------- Graph Reading in both orientations (i,j) and (j,i) -----------#
def graph_read(graph_file, time, delimiter):
	cur.execute("CREATE TABLE E_s (i int ENCODING RLE, j int ENCODING RLE, PRIMARY KEY(i,j));")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_super(i ENCODING RLE, j ENCODING RLE) AS SELECT i,j FROM E_s ORDER BY i,j SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	# read the graph CSV file (list of edges) to insert it into table E_s
	if (delimiter=="space"):
		cur.execute("COPY E_s FROM '"+graph_file+"' delimiter ' ';")
	elif (delimiter=="comma"):
		cur.execute("COPY E_s FROM '"+graph_file+"' delimiter ',';")
	else : 
		cur.execute("COPY E_s FROM '"+graph_file+"' delimiter '\t';")
	time = time + get_time(cur)
	cur.execute("COMMIT;")
	cur.execute("INSERT INTO E_s SELECT j,i FROM E_s;");
	time = time + get_time(cur)
	cur.execute("COMMIT;")
	return time

#---------- Quadruplet Generation (for 4,8,9,16,27,81 machines) -------------#
def quad_gen(k):
	quadruplet = pd.DataFrame()
	quad4 = {'machine':[1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4], 
		'c1':[1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2], 
		'c2':[1,1,1,1,2,2,2,2,1,1,1,1,2,2,2,2], 
		'c3':[1,1,2,2,1,1,2,2,1,1,2,2,1,1,2,2], 
		'c4':[1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2]}
	quad8 = {'machine':[1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8], 
		'c1':[1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2], 
		'c2':[1,1,1,1,2,2,2,2,1,1,1,1,2,2,2,2], 
		'c3':[1,1,2,2,1,1,2,2,1,1,2,2,1,1,2,2],
		'c4':[1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2]}
	quad16 = {'machine':[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16], 
		'c1':[1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2], 
		'c2':[1,1,1,1,2,2,2,2,1,1,1,1,2,2,2,2], 
		'c3':[1,1,2,2,1,1,2,2,1,1,2,2,1,1,2,2],
		'c4':[1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2]}
	quad9 = {'machine':[1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,4,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5,5,6,6,6,6,6,6,6,6,6,7,7,7,7,7,7,7,7,7,8,8,8,8,8,8,8,8,8,9,9,9,9,9,9,9,9,9],
		'c1':[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],
		'c2':[1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3],
		'c3':[1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3],
		'c4':[1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3]}
	quad27 = {'machine':[1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6,7,7,7,8,8,8,9,9,9,10,10,10,11,11,11,12,12,12,13,13,13,14,14,14,15,15,15,16,16,16,17,17,17,18,18,18,19,19,19,20,20,20,21,21,21,22,22,22,23,23,23,24,24,24,25,25,25,26,26,26,27,27,27],
		'c1':[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],
		'c2':[1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3],
		'c3':[1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3],
		'c4':[1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3]}
	quad81 = {'machine':[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81],
		'c1':[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],
		'c2':[1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3],
		'c3':[1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3,1,1,1,2,2,2,3,3,3],
		'c4':[1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3]}
	if k==4 :
		quadruplet = pd.DataFrame(data=quad4, index=None)
	elif k==8 :
		quadruplet = pd.DataFrame(data=quad8, index=None)
	elif k==16 :
		quadruplet = pd.DataFrame(data=quad16, index=None)
	elif k==9 :
		quadruplet = pd.DataFrame(data=quad9, index=None)
	elif k==27 :
		quadruplet = pd.DataFrame(data=quad27, index=None)
	else :
		quadruplet = pd.DataFrame(data=quad81, index=None)
	return quadruplet;

#----------------- Graph Partitioning For Triangles ------------------#
def graph_color(k, time,c):
	#Assign random color to V
	drop_table("V_s")
        time = time + get_time(cur)
	cur.execute("CREATE TABLE V_s(i int,color int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION V_s_super(i, color ENCODING RLE) AS SELECT i, color FROM V_s ORDER BY i SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO V_s SELECT i,randomint("+str(c)+")+1 FROM (SELECT DISTINCT i FROM E_s UNION SELECT DISTINCT j FROM E_s)V;")
	time = time + get_time(cur)
	cur.execute("COMMIT;")

	#Color edges based on colored vertices, edges are repartitioned
	drop_table("E_s_proxy")
        time = time + get_time(cur)
	cur.execute("CREATE TABLE E_s_proxy(i_color int NOT NULL,j_color int NOT NULL,i int NOt NULL,j int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_proxy_super(i_color ENCODING RLE, j_color ENCODING RLE, i, j) AS SELECT i_color,j_color,i,j FROM E_s_proxy ORDER BY i,j SEGMENTED BY hash(i_color,j_color) ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO E_s_proxy SELECT Vi.color, Vj.color,E.i,E.j FROM E_s E JOIN V_s Vi ON E.i=Vi.i JOIN V_s Vj ON E.j=Vj.i;")
	time = time + get_time(cur)
	cur.execute("COMMIT;")
	time1 = create_quadruplet(k,time)
	return time

def create_quadruplet(k,time):
	drop_table("quadruplet")
        time = time + get_time(cur)
	cur.execute("CREATE TABLE quadruplet(c1 int, c2 int, c3 int, c4 int, machine int);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION quadruplet_super(c1, c2, c3, c4, machine) AS SELECT c1, c2, c3, c4, machine FROM quadruplet ORDER BY machine UNSEGMENTED ALL NODES;")
	time = time + get_time(cur)
	quad = quad_gen(k)
	buff = cStringIO.StringIO()
	for row in quad.values.tolist():
		buff.write('{}|{}|{}|{}|{}\n'.format(*row))

	cur.copy('COPY quadruplet(c1, c2, c3, c4, machine) FROM STDIN COMMIT' , buff.getvalue()) 
	time = time + get_time(cur)
	return time

#--------------------- Wedge Enumeration ----------------------#
#Wedge of type 1 (u,v,w)
def enumerate_wedges_T1(k):
	#Collect required wedges from proxies
	drop_table("E_s_local")
        time1 = get_time(cur)
	cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL, CONSTRAINT id PRIMARY KEY (machine, i, j) ENABLED);")
	time1 = time1 + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time1 = time1 + get_time(cur)
	cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c1 and E.j_color=edge1.c2 UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge2 ON E.i_color=edge2.c2 and E.j_color=edge2.c3;")
	time1 = time1 + get_time(cur)
	cur.execute("COMMIT;")
	
	#Wedge enumeration
	drop_table("Wedge_dup")
	time2 = get_time(cur)
	cur.execute("CREATE TABLE Wedge_dup(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL);")
	time2 = time2 + get_time(cur)
	cur.execute("CREATE PROJECTION Wedge_dup_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge_dup ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time2 = time2 + get_time(cur)
	cur.execute("INSERT INTO Wedge_dup SELECT E1.machine as machine, E1.i as u, E1.j as v, E2.j as w, E1.i_color as u_color, E1.j_color as v_color, E2.j_color as w_color FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i WHERE E1.i!=E2.j;")
	time2 = time2 + get_time(cur)
	cur.execute("COMMIT;")

	#Repetition elimination
	drop_table("Wedge")
	time3 = get_time(cur)
	cur.execute("CREATE TABLE Wedge(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL, CONSTRAINT Wedge_id PRIMARY KEY (machine, u,v,w) ENABLED);")
	time3 = time3 + get_time(cur)
	cur.execute("CREATE PROJECTION Wedge_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time3 = time3 + get_time(cur)
	cur.execute("insert into Wedge SELECT DISTINCT W.machine as machine, u,v,w,u_color,v_color,w_color from Wedge_dup W JOIN quadruplet Q ON W.machine=Q.machine and W.u_color=Q.c1 and W.v_color=Q.c2 and W.w_color=Q.c3;")
	time3 = time3 + get_time(cur)
	cur.execute("COMMIT;")
	time = time1 + time2 + time3
	return time

#Wedge of type 2 (v,w,z)
def enumerate_wedges_T2(k):
	#Collect required wedges from proxies
        drop_table("E_s_local")
        time1 = get_time(cur)
        cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL, CONSTRAINT id PRIMARY KEY (machine, i, j) ENABLED);")
        time1 = time1 + get_time(cur)
        cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
        time1 = time1 + get_time(cur)
        cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c2 and E.j_color=edge1.c3 UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge2 ON E.i_color=edge2.c3 and E.j_color=edge2.c4;")
        time1 = time1 + get_time(cur)
        cur.execute("COMMIT;")
	
	#Wedge enumeration
	drop_table("Wedge_dup")
	time2 = get_time(cur)
	cur.execute("CREATE TABLE Wedge_dup(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL);")
	time2 = time2 + get_time(cur)
	cur.execute("CREATE PROJECTION Wedge_dup_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge_dup ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time2 = time2 + get_time(cur)
	cur.execute("INSERT INTO Wedge_dup SELECT E1.machine as machine, E1.i as u, E1.j as v, E2.j as w, E1.i_color as u_color, E1.j_color as v_color, E2.j_color as w_color FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i WHERE E1.i!=E2.j;")
	time2 = time2 + get_time(cur)
	cur.execute("COMMIT;")
	
	#Repetition elimination
        drop_table("Wedge_sup")
        time3 = get_time(cur)
        cur.execute("CREATE TABLE Wedge_sup(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL, CONSTRAINT wedge_sup_id PRIMARY KEY (machine, u,v,w) ENABLED);")
        time3 = time3 + get_time(cur)
        cur.execute("CREATE PROJECTION Wedge_sup_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge_sup ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
        time3 = time3 + get_time(cur)
        cur.execute("INSERT INTO Wedge_sup SELECT DISTINCT W.machine as machine,u, v, w, u_color, v_color, w_color FROM Wedge_dup W JOIN quadruplet Q ON W.machine=Q.machine AND W.u_color=Q.c2 AND W.v_color=Q.c3  AND W.w_color=Q.c4;")
        time3 = time3 + get_time(cur)
        cur.execute("COMMIT;")
        time = time1 + time2 + time3
        return time

#Wedge of type 3 (u,v,z)
def enumerate_wedges_T3(k):
	#Collect required wedges from proxies
	drop_table("E_s_local")
        time1 = get_time(cur)
        cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL, CONSTRAINT id PRIMARY KEY (machine, i, j) ENABLED);")
        time1 = time1 + get_time(cur)
        cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
        time1 = time1 + get_time(cur)
        cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c1 and E.j_color=edge1.c2 UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge2 ON E.i_color=edge2.c2 and E.j_color=edge2.c4;")
        time1 = time1 + get_time(cur)
        cur.execute("COMMIT;")
	
	#Wedge enumeration
	drop_table("Wedge_dup")
	time2 = get_time(cur)
	cur.execute("CREATE TABLE Wedge_dup(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL);")
	time2 = time2 + get_time(cur)
	cur.execute("CREATE PROJECTION Wedge_dup_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge_dup ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time2 = time2 + get_time(cur)
	cur.execute("INSERT INTO Wedge_dup SELECT E1.machine as machine, E1.i as u, E1.j as v, E2.j as w, E1.i_color as u_color, E1.j_color as v_color, E2.j_color as w_color FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i WHERE E1.i!=E2.j;")
	time2 = time2 + get_time(cur)
	cur.execute("COMMIT;")

	#Repetition elimination
        drop_table("Wedge_st")
        time3 = get_time(cur)
        cur.execute("CREATE TABLE Wedge_st(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL, CONSTRAINT wedge_st_id PRIMARY KEY (machine, u,v,w) ENABLED);")
        time3 = time3 + get_time(cur)
        cur.execute("CREATE PROJECTION Wedge_st_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge_st ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
        time3 = time3 + get_time(cur)
        cur.execute("INSERT INTO Wedge_st SELECT DISTINCT W.machine as machine,u, v, w, u_color, v_color, w_color FROM Wedge_dup W JOIN quadruplet Q ON W.machine=Q.machine AND W.u_color=Q.c1 AND W.v_color=Q.c2 AND W.w_color=Q.c4;")
        time3 = time3 + get_time(cur)
        cur.execute("COMMIT;")
        time = time1 + time2 + time3
        return time

#Wedge of type 4 (w,z,u)
def enumerate_wedges_T4(k):
	drop_table("E_s_local")
        time1 = get_time(cur)
	cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL, CONSTRAINT id PRIMARY KEY (machine, i, j) ENABLED);")
	time1 = time1 + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time1 = time1 + get_time(cur)
	cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c3 and E.j_color=edge1.c4 UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge2 ON E.i_color=edge2.c4 and E.j_color=edge2.c1;")
	time1 = time1 + get_time(cur)
	cur.execute("COMMIT;")
	
	#Wedge enumeration
	drop_table("Wedge_dup")
	time2 = get_time(cur)
	cur.execute("CREATE TABLE Wedge_dup(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL);")
	time2 = time2 + get_time(cur)
	cur.execute("CREATE PROJECTION Wedge_dup_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge_dup ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time2 = time2 + get_time(cur)
	cur.execute("INSERT INTO Wedge_dup SELECT E1.machine as machine, E1.i as u, E1.j as v, E2.j as w, E1.i_color as u_color, E1.j_color as v_color, E2.j_color as w_color FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i WHERE E2.j!=E1.i;")
	time2 = time2 + get_time(cur)
	cur.execute("COMMIT;")
	
	#Repetition elimination
	drop_table("Wedge_op")
	time3 = get_time(cur)
	cur.execute("CREATE TABLE Wedge_op(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, u_color int NOT NULL,v_color int NOT NULL, w_color int NOT NULL, CONSTRAINT wedge_op_id PRIMARY KEY (machine, u,v,w) ENABLED);")
	time3 = time3 + get_time(cur)
	cur.execute("CREATE PROJECTION Wedge_op_super(machine ENCODING RLE, u, v, w, u_color, v_color, w_color) AS SELECT machine,u, v, w, u_color, v_color, w_color FROM Wedge_op ORDER BY u,v,w SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time3 = time3 + get_time(cur)
	cur.execute("INSERT INTO Wedge_op SELECT DISTINCT W.machine as machine,u, v, w, u_color, v_color, w_color FROM Wedge_dup W JOIN quadruplet Q ON W.machine=Q.machine AND W.u_color=Q.c3 AND W.v_color=Q.c4  AND W.w_color=Q.c1;")
	time3 = time3 + get_time(cur)
	cur.execute("COMMIT;")
	time = time1 + time2 + time3
	return time

#----------------------------- 3 Path ------------------------------#
def enumerate_3_path(k):
	drop_table("Path")
	time = get_time(cur)
	cur.execute("CREATE TABLE Path(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, z int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION Path_super(machine ENCODING RLE, u, v, w, z) AS SELECT machine,u, v, w, z FROM Path ORDER BY u,v,w,z SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO Path SELECT P.machine as machine, P.u as u, P.v as v, P.w as w, E.w as z FROM Wedge P JOIN Wedge_sup E ON P.machine=E.machine AND P.v=E.u AND P.w=E.v WHERE P.u<E.w;")
	time = time + get_time(cur)
	cur.execute("COMMIT")
	return time

#------------------------------ 3 star ----------------------------#
def enumerate_3_star(k):
	drop_table("Star")
	time = get_time(cur)
	cur.execute("CREATE TABLE Star(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, z int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION Star_super(machine ENCODING RLE, u, v, w, z) AS SELECT machine,u, v, w, z FROM Star ORDER BY u,v,w,z SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO Star SELECT S.machine as machine, S.u as u, S.v as v, S.w as w, E.w as z FROM Wedge S JOIN Wedge_st E ON S.machine=E.machine AND S.u=E.u AND S.v=E.v WHERE S.u<S.w AND S.w<E.w;")
	time = time + get_time(cur)
	cur.execute("COMMIT")
	return time

#----------------------------- Rectangle ----------------------------#
def enumerate_rectangle(k):
	drop_table("Rectangle")
	time = get_time(cur)
	cur.execute("CREATE TABLE Rectangle(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, z int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION Rectangle_super(machine ENCODING RLE, u, v, w, z) AS SELECT machine,u, v, w, z FROM Rectangle ORDER BY u,v,w,z SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO Rectangle SELECT R.machine as machine, R.u as u, R.v as v, R.w as w, T.v as z from Wedge R join Wedge_op T ON R.machine=T.machine AND R.u=T.w AND R.w=T.u WHERE R.u<R.v AND R.v<R.w AND R.w<T.v UNION SELECT R.machine as machine, R.u as u, R.v as v, R.w as w, T.v as z from Wedge R join Wedge_op T ON R.machine=T.machine AND R.u=T.w AND R.w=T.u WHERE R.u<R.v AND R.v<T.v AND T.v<R.w UNION SELECT R.machine as machine, R.u as u, R.v as v, R.w as w, T.v as z from Wedge R join Wedge_op T ON R.machine=T.machine AND R.u=T.w AND R.w=T.u WHERE R.u<R.w AND R.w<R.v AND R.v<T.v;")
	time = time + get_time(cur)
	cur.execute("COMMIT")
	return time	
	

#------------------------------ Tailed Triangles---------------------#
def enumerate_tailed_triangle(k):
	drop_table("E_s_local")
        time1 = get_time(cur)
	cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL);")
	time1 = time1 + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time1 = time1 + get_time(cur)
	cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c1 and E.j_color=edge1.c3 UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge2 ON E.i_color=edge2.c1 and E.j_color=edge2.c4 UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge3 ON E.i_color=edge3.c3 and E.j_color=edge3.c4;")
	time1 = time1 + get_time(cur)
	cur.execute("COMMIT;")
	
	drop_table("Tailed")
	time = get_time(cur)
	cur.execute("CREATE TABLE Tailed(type int NOT NULL, machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, z int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION Tailed_super(type ENCODING RLE, machine ENCODING RLE, u, v, w, z) AS SELECT type, machine,u, v, w, z FROM Tailed ORDER BY u,v,w,z SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO Tailed SELECT 1, R.machine, R.u as u, R.v as v, R.w as w, R.z as z FROM Star R Join E_s_local E1 ON R.machine=E1.machine AND R.u=E1.i AND R.w=E1.j UNION SELECT 2, R.machine, R.u as u, R.v as v, R.w as w, R.z as z FROM Star R Join E_s_local E2 ON R.machine=E2.machine AND R.u=E2.i AND R.z=E2.j UNION SELECT 3, R.machine, R.u as u, R.v as v, R.w as w, R.z as z FROM Star R Join E_s_local E3 ON R.machine=E3.machine AND R.w=E3.i AND R.z=E3.j;")
	time = time + get_time(cur)
	return time

#----------------------------- Diamond ------------------------------#
def enumerate_diamond(k):
	drop_table("E_s_local")
	time1 = get_time(cur)
	cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL);")
	time1 = time1 + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time1 = time1 + get_time(cur)
	cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c1 and E.j_color=edge1.c3 Where E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c2 and E.j_color=edge1.c4 where E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN quadruplet edge1 ON E.i_color=edge1.c4 and E.j_color=edge1.c1 where E.i>E.j;")
	time1 = time1 + get_time(cur)
	cur.execute("COMMIT;")
	
	drop_table("Diamond")
	time = get_time(cur)
	cur.execute("CREATE TABLE Diamond(machine int NOT NULL, u int NOT NULL,v int NOT NULL, w int NOT NULL, z int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION Diamond_super(machine ENCODING RLE, u, v, w, z) AS SELECT machine,u, v, w, z FROM Diamond ORDER BY u,v,w,z SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO Diamond SELECT R.machine, u, v, w, z FROM Rectangle R Join E_s_local E1 ON R.machine=E1.machine AND R.u=E1.i AND R.w=E1.j UNION SELECT R.machine, u, v, w, z FROM Rectangle R Join E_s_local E1 ON R.machine=E1.machine AND E1.i=R.v AND E1.j=R.Z;")
	time = time + get_time(cur)
	cur.execute("COMMIT")
	return time

#----------------------------- Clique -------------------------------#
def enumerate_4_node(k):	
	drop_table("Clique")
	time = get_time(cur)
	cur.execute("CREATE TABLE Clique(machine int NOT NULL,u int NOT NULL,v int NOT NULL, w int NOT NULL, z int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION Clique_super(machine ENCODING RLE, u, v, w, z) AS SELECT machine,u, v, w, z FROM Clique ORDER BY u,v,w,z SEGMENTED BY (machine*4294967295//"+str(k)+") ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO Clique SELECT R.machine, u, v, w, z FROM Rectangle R Join E_s_local E1 ON R.machine=E1.machine AND R.u=E1.i AND R.w=E1.j Join E_s_local E2 ON R.machine=E2.machine AND R.v=E2.i AND R.z=E2.j WHERE R.u<R.v AND R.v<R.w AND R.w<R.z;")
	time = time + get_time(cur)
	cur.execute("COMMIT;")
	return time

#----------------------------- The main -----------------------------# 
if __name__ == "__main__":
	#Database Connection
	try:
		conn_info = {'database':'graph',
			'port': 5433,
			'user': 'dbadmin'
			}
 
		# simple connection, with manual close
		connection = vp.connect(**conn_info)
		cur = connection.cursor()
	except:
		print("Database connection error")
		sys.exit()
	
	#Reading input
	inputs = sys.argv[1]
	param = inputs.split(',')
	print(len(param))
	print(inputs)
	if (len(param)!=4):
		print("please insert the correct arguement as defined in the documentation:")
		print(">> python sqlgraphlet.py file=/link/to/file,delimiter=[tab|comma|space],machine=k,color=c")
		sys.exit()
	else:
		graph_file = param[0].split('=')[1]
		delimiter = param[1].split('=')[1]
		k = param[2].split('=')[1] 
		c = param[3].split('=')[1]

	time = 0
	
	#Graph reading
	cur.execute("SELECT CLEAR_CACHES();")
	drop_table("E_s")
	time = graph_read(graph_file,time,delimiter)
	
	#Graph coloring
	time = graph_color(k, time,c)
	print("Partitioning time is " + str(time))
	print(" ")
	
	#---------------- Wedges ---------------#
	#Wedge enumeration
	time3 = enumerate_wedges_T1(k)
	print("Total Wedge_T1 (u,v,w) enum time is " +str(time3))
	time3_1 = enumerate_wedges_T2(k)
	print("Total Wedge_T2 (v,w,z) enum time is " +str(time3_1))
	time3_2 = enumerate_wedges_T3(k)
	print("Total Wedge_T3 (u,v,z) enum time is " +str(time3_2))
	time3_3 = enumerate_wedges_T4(k)
	print("Total Wedge_T4 (w,z,u) enum time is " +str(time3_3))
	print(" ")
	
	#---------------- Intuitive Graphlets ---------------#
	#3_Path
	time5 = enumerate_3_path(k)
	print("Total 3 path enum time is " +str(time5))
	cur.execute("select count(*) from Path;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		print("3-Path count is "+str(rows[0][0]))
	print("  ")
	
	#3_Star
	time6= enumerate_3_star(k)
	print("Total 3 Star enum time is " +str(time6))
	cur.execute("select count(*) from Star;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		print("3-Star count is "+str(rows[0][0]))
	print("  ")
	
	#Rectangle
	time7= enumerate_rectangle(k)
	print("Total rectangle enum time is " +str(time7))
	cur.execute("select count(*) from Rectangle;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		print("Rectangle count is "+str(rows[0][0]))
	print("  ")
	
	#intuitive graphlet time
	timei = time5 + time6 + time7 + time3
	print("Total intuitive time is "+str(timei))
	
	#---------------- Derived Graphlets ---------------#
	#Tailed_Triangle
	time8 = enumerate_tailed_triangle(k)
	print("Total Tailed TR enum time is " +str(time8))
	cur.execute("select count(*) from Tailed;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		print("Tailed triangle count is "+str(rows[0][0]))
	print("  ")
	
	#Diamond
	time9= enumerate_diamond(k)
	print("Total diamond enum time is " +str(time9))
	cur.execute("select count(*) from Diamond;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		print("Diamond count is "+str(rows[0][0]))
	print("  ")
	
	#Clique
	time10= enumerate_4_node(k)
	print("Total clique enum time is " +str(time10))
	cur.execute("select count(*) from Clique;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		print("Clique count is "+str(rows[0][0]))
	print("  ")
	
	#Derived graphlet time
	timed = time8 + time9 + time10
	print("Total Derived enum time is " +str(timed))
	
	#---------------- Total Time ---------------#
	#Total time
	timet = timei + timed
	print("Total Derived enum time is " +str(timet))
