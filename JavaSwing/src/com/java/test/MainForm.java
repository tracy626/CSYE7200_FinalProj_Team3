package com.java.test;

import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.Array;
import java.util.Arrays;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

public class MainForm {

	private JFrame frame;
	static DB database;
	static Mongo mongo;
	private JScrollPane scrollPane;
	private static JTable jtUsers;
	private JTextField txtFilterAvg;
	private JTextField txtFilterALS;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			@Override
			public void run() {
				try {
					MainForm window = new MainForm();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		mongo = new MongoClient("localhost", 27017);
		database = mongo.getDB("csye7200");
		loadTable1(null);
	}

	private static void loadTable1(BasicDBObject _filterQuery) {
		DBCollection dbcol = database.getCollection("AverageMovies");
		DBCursor result = _filterQuery!=null 
				? dbcol.find(_filterQuery) 
				: dbcol.find();
		Object results[][] = new Object[result.count()][];
		int index = 0;
		while (result.hasNext()) {
			DBObject rr = result.next();
			results[index] = new Object[] { rr.get("mid").toString(),
					rr.get("avg").toString(),
					rr.get("_id").toString() };
			index++;
		}
		TableModel tablemodel = new DefaultTableModel(results, new Object[] {
				"mid", "avg", "_id" });
		jtUsers.setModel(tablemodel);
		jtUsers.getColumn("_id").setWidth(0);
		jtUsers.getColumn("_id").setMinWidth(0);
	}




	private static void loadTable2(BasicDBObject _filterQuery) {
		DBCollection dbcol = database.getCollection("UserRecs");
		DBCursor result = _filterQuery!=null
				? dbcol.find(_filterQuery)
				: dbcol.find();
		Object results[][] = new Object[20][];
//  Array rec [][]= new Arr;


		while (result.hasNext()) {

			DBObject rr = result.next();
			int index =0;
			for (int i = 0; i < 20; i++) {
				results[i] = new Object[] {
						rr.get("uid").toString(),
						Arrays.asList(rr.get("recs").toString().trim().replace("[ ","").replace("{ \"mid\" : ","").replace(", \"score\" :","").split("} ,|  ")).get(index),
						Arrays.asList(rr.get("recs").toString().trim().replace("[ ","").replace("{ \"mid\" : ","").replace(", \"score\" :","").split("} ,|  ")).get(index+1)

				};
				index += 2;
			}
		}
		TableModel tablemodel = new DefaultTableModel(results, new Object[] {
				"uid", "mid", "score"});
		jtUsers.setModel(tablemodel);
		jtUsers.getColumn("_id").setWidth(0);
	}

	public MainForm() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 800, 600);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);

		scrollPane = new JScrollPane();
		scrollPane.setBounds(11, 11, 700, 350);
		frame.getContentPane().add(scrollPane);

		jtUsers = new JTable();
		jtUsers.setFillsViewportHeight(true);
		scrollPane.setViewportView(jtUsers);

		JButton btnFilter = new JButton("Filter");
		btnFilter.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				loadTable1(txtFilterAvg.getText().trim().length()==0 ? null : new BasicDBObject("avg", Double.parseDouble(txtFilterAvg.getText())));
			}
		});
		btnFilter.setBounds(250, 400, 89, 23);
		frame.getContentPane().add(btnFilter);

		JLabel label = new JLabel("Ave:");
		label.setHorizontalAlignment(SwingConstants.RIGHT);
		label.setBounds(10, 400, 46, 14);
		frame.getContentPane().add(label);

		txtFilterAvg = new JTextField();
		txtFilterAvg.setColumns(10);
		txtFilterAvg.setBounds(100, 400, 93, 20);
		frame.getContentPane().add(txtFilterAvg);










































//		scrollPane2 = new JScrollPane();
//		scrollPane2.setBounds(11, 11, 500, 350);
//		frame.getContentPane().add(scrollPane);

		jtUsers = new JTable();
		jtUsers.setFillsViewportHeight(true);
		scrollPane.setViewportView(jtUsers);





		JButton btnFilter2 = new JButton("Filter");
		btnFilter2.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg1) {
				loadTable2(txtFilterALS.getText().trim().length()==0 ? null : new BasicDBObject("uid", Integer.parseInt(txtFilterALS.getText())));
			}
		});
		btnFilter2.setBounds(250, 450, 89, 23);
		frame.getContentPane().add(btnFilter2);

		JLabel label2 = new JLabel("UserID:");
		label2.setHorizontalAlignment(SwingConstants.RIGHT);
		label2.setBounds(10, 450, 46, 14);
		frame.getContentPane().add(label2);

		txtFilterALS = new JTextField();
		txtFilterALS.setColumns(10);
		txtFilterALS.setBounds(100, 450, 93, 20);
		frame.getContentPane().add(txtFilterALS);
	}






}


