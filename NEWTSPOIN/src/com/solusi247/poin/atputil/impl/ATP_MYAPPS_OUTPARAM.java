package com.solusi247.poin.atputil.impl;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Driver;
import javax.servlet.ServletException;
import oracle.jdbc.pool.OracleDataSource;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import com.solusi247.poin.Helper;

public class ATP_MYAPPS_OUTPARAM {
	private Helper helper = new Helper();
	private static String v_date_trx="";
	private static String kwd;
	private static String kwdv;
	private static String merchant;
	private static String vmsg_out;
	private static String merchant_id;
	public ATP_MYAPPS_OUTPARAM(){

		v_date_trx=helper.dateToString("dd/MM/yyyy", new Date());
	
		Object atp_util;
		String c_kwd = "select * from atp_keywords where keyword_id = " + atp_util.VKEYWORD_ID;
		String c_kwdv = "select * from atp_keywords where keyword_master = " + atp_util.VKEYWORD;
		

	
		ResultSet r_kwd = statement.executeQuery(c_kwd);
		if (r_kwd!=null){
		
			kwd=r_kwd.getString(exp_date);
			merchant_id=r_kwd.getString(merchant_id);
		}
		
		ResultSet r_kwdv = statement.executeQuery(c_kwdv);
		if (r_kwdv!=null){
		
			kwdv=r_kwdv.getString(keyword);
		}

		String c_merchant = "select * from atp_merchant where merchant_id = " + r_kwd.merchant_id;
		ResultSet c_merchant = statement.executeQuery(c_merchant);
		if (r_merchant!=null){
		
			merchant=r_merchant.getString(MERCHANT_NAME);

		}
		
		
		
		if (atp_util.vuserid == "MYAPPS-RA" || atp_util.vuserid == "MYAPPS_ANDROID-RA"){
			atp_util.vmsg_out = atp_util.vmsg_out + "|~|" + atp_util.vbu_txt + "|" + r_kwdv + "|" + r_kwd + "|" + atp_util.vstatus;
		}
		
		atp_util.vmsg_out=atp_util.vmsg_out.replace("[poin_redeem]", atp_util.vpoint_usg);
		atp_util.vmsg_out=atp_util.vmsg_out.replace("[poin_redeem]", atp_util.vpoint_usg);
		atp_util.vmsg_out=atp_util.vmsg_out.replace("[product_name]", atp_util.vprod_name);
		atp_util.vmsg_out=atp_util.vmsg_out.replace("[merchant_name]", MERCHANT_NAME);
		atp_util.vmsg_out=atp_util.vmsg_out.replace("[voucher_code]", atp_util.vbu_txt);
		atp_util.vmsg_out=atp_util.vmsg_out.replace("[msisdn]", atp_util.vmsisdn);
		atp_util.vmsg_out=atp_util.vmsg_out.replace("[date_trx]", v_date_trx);
	
	}
}
