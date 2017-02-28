package com.solusi247.poin.atputil.impl;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.jboss.logging.Logger;

import com.solusi247.mastertselpoin.helper.Constant;
import com.solusi247.mastertselpoin.helper.Helper;
import com.solusi247.mastertselpoin.helper.ManagementDataCache;
import com.solusi247.mastertselpoin.helper.ParameterKey;
import com.solusi247.poin.atputil.impl.PRC_CP_POSTPAID_REFUND;

public class JBOSS_PRC_CP_POSTPAID_REFUND {
	static private Logger logger = Logger.getLogger(JBOSS_PRC_CP_POSTPAID_REFUND.class);
	private ManagementDataCache cache = new ManagementDataCache();
	private Helper helper = new Helper();
	String PTRX_ID;//        IN VARCHAR2,
	String PORDER_ID;//       IN VARCHAR2,
	String PORDER_STATUS;//      IN VARCHAR2,
	String PCHANNEL;//      IN VARCHAR2,
	String PMSISDN_IN;//   IN VARCHAR2,

	String PERR_CODE;//       OUT VARCHAR2,
	String PERR_MSG;//      OUT VARCHAR2,
	String XP_MSISDN;//      OUT VARCHAR2,
	String P_NOTIF;//      OUT VARCHAR2
	
	public PRC_CP_POSTPAID_REFUND execute(Jboss_Atp_Util atp_util) {
		String VCMD;//	     VARCHAR2(2000);
		int VLENGTH=0;//    NUMBER:=0;
		String VSQLERR;//    VARCHAR2(200);
		int VSQLCODE;//   NUMBER(10);
		SimpleDateFormat format1 = new SimpleDateFormat("YYYYMMDD");
		
		int vcek= 0;
		int vcpts= 0;
		int v_id;
		String VURL=null;
		String VRETVAL=null;
		String VREQ=null;
		String VLOGID;
		String P_API_NAME="";
		String VTRXID="";
		String VHTPP="";
		String pmsisdn="";
		String pmsgtext="";
		String VKEYWORD;
		String vperiod;
		String PT_ID_ATP_KEYWORDS = null;
		String KEYWORD_ID_ATP_KEYWORDS = null;
		String KEYWORD_ATP_KEYWORDS = null;
		String EFF_DATE_ATP_KEYWORDS = null;
		String EXP_DATE_ATP_KEYWORDS = null;
		String EVT_ID_REFF_ATP_KEYWORDS = null;
		String PROGRAM_ID_ATP_KEYWORDS = null;
		String RESP_CODE_ATP_PARAMETER_MSG_NON = null;
		String MSG_TEXT_ATP_PARAMETER_MSG_NON = null;
		String MSG_CODE_ATP_PARAMETER_MSG_NON = null;
		String MSG_TYPE_ATP_PARAMETER_MSG_NON = null;
		String MSG_SEQ_ATP_PARAMETER_MSG_NON = null;

		String XERR_CODE;
		String XERR_MSG;
		VTRXID= PTRX_ID;
		pmsisdn= PMSISDN_IN;
		pmsgtext= "";
		VKEYWORD= "";
		int HITROW_ATP_KEYWORDS;
		String sql;
		PreparedStatement ps = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String upd;
		logger.info("trxid: " + VTRXID);
		Calendar calendar = Calendar.getInstance();
		vperiod = format1.format(calendar.getTime());
		  //lakukan pengembalian poin
		

		// memanggil class //
		JBOSS_PRC_REFUND_POIN PRC_REFUND_POIN = new JBOSS_PRC_REFUND_POIN();
		JBOSS_PRC_REFUND_POIN.PRC_REFUND_POIN(PTRX_ID,PCHANNEL,PMSISDN_IN,XERR_CODE,XERR_MSG,XP_MSISDN,VKEYWORD,vperiod);
		P_NOTIF = "";

		 
		 if (XERR_CODE != "000"){
			    PERR_CODE= XERR_CODE;
			    PERR_MSG= XERR_MSG;
			    P_NOTIF= "";
			  }else{
				  
				    d_keyword_detail= null;
					listParameter.add(new ParameterKey(Constant.AK_KEYWORD,VKEYWORD));
					listParameter.add(new ParameterKey(Constant.AK_STATUS, "1"));
					String resultAk = cache.getSingleDataHashMap(listParameter,"ATP_KEYWORDS");

					if (null != resultAk) {
						String arryResultAk[] = resultAk.split("\\|");

						PT_ID_ATP_KEYWORDS = arryResultAk[Constant.VAK_PT_ID];
						KEYWORD_ID_ATP_KEYWORDS = arryResultAk[Constant.VAK_KEYWORD_ID];
						KEYWORD_ATP_KEYWORDS = arryResultAk[Constant.VAK_KEYWORD];
						EFF_DATE_ATP_KEYWORDS = arryResultAk[Constant.VAK_EFF_DATE];
						EXP_DATE_ATP_KEYWORDS = arryResultAk[Constant.VAK_EXP_DATE];
						EVT_ID_REFF_ATP_KEYWORDS = arryResultAk[Constant.VAK_EVT_ID_REFF];
						PROGRAM_ID_ATP_KEYWORDS = arryResultAk[Constant.VAK_PROGRAM_ID];

						Date effDate = helper.getDateFromString("dd-MMM-yyyy",EFF_DATE_ATP_KEYWORDS);
						Date expDate = helper.getDateFromString("dd-MMM-yyyy",EXP_DATE_ATP_KEYWORDS);

						if (new Date().compareTo(effDate) >= 0
								&& expDate.compareTo(new Date()) >= 0) {
							HITROW_ATP_KEYWORDS = 1;
							
						} else {
							HITROW_ATP_KEYWORDS = 0;
							PT_ID_ATP_KEYWORDS=null;
							KEYWORD_ID_ATP_KEYWORDS=null;
							KEYWORD_ATP_KEYWORDS=null;
							EFF_DATE_ATP_KEYWORDS=null;
							EXP_DATE_ATP_KEYWORDS=null;
							EVT_ID_REFF_ATP_KEYWORDS=null;
							PROGRAM_ID_ATP_KEYWORDS=null;
	
							Date effDate=null;
							Date expDate=null;
						}

					}
	
					if (KEYWORD_ATP_KEYWORDS!=null){
						sql = "select * from  atp_max_redeem where msisdn = " + pmsisdn + " and evt_id = " + EVT_ID_REFF_ATP_KEYWORDS + " and period = " + vperiod;
						
						try{
							Class.forName("oracle.jdbc.OracleDriver");
							String dbURL = "jdbc:oracle:thin:@10.2.232.93:1521:ophpoint";
							String username = "newtspoin";
							String password = "newtspoin";

							conn = DriverManager.getConnection(dbURL, username,password);
							stmt = conn.createStatement();
							rs = stmt.executeQuery(sql);
							int nvl=0;
							while (rs.next()) {
								nvl=String.valueOf(rs.getint(total));
								if (String.valueOf(rs.getString(msisdn))!=null && nvl > 0){
									//do decrement
								upd="update atp_max_redeem set total = nvl(total,0) - 1 where msisdn = " + String.valueOf(rs.getString(msisdn)) + " and evt_id = " + EVT_ID_REFF_ATP_KEYWORDS + " and period = " + vperiod;
								stmt = conn.createStatement();
								stmt.executeUpdate(upd);
								}
							}

							if (null != rs)
								rs.close();
							if (null != conn)
								conn.close();
							if (null != stmt)
								stmt.close();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (SQLException e) {
							e.printStackTrace();
						} finally {
							try {
								if (null != rs)
									rs.close();
								if (null != conn)
									conn.close();
								if (null != stmt)
									stmt.close();
							} catch (SQLException e1) {
								e1.printStackTrace();
							}
						}
				
						sql = "select * from atp_max_redeem where msisdn = " + pmsisdn + " and evt_id = " + PROGRAM_ID_ATP_KEYWORDS + " and period =  " + vperiod;
						try {
							Class.forName("oracle.jdbc.OracleDriver");

							String dbURL = "jdbc:oracle:thin:@10.2.232.93:1521:ophpoint";
							String username = "newtspoin";
							String password = "newtspoin";

							conn = DriverManager.getConnection(dbURL, username,password);
							stmt = conn.createStatement();
							rs = stmt.executeQuery(sql);
							nvl=0;
							while (rs.next()) {
								atp_util.VTKN_ID = String.valueOf(rs.getInt(1));
								nvl=String.valueOf(rs.getInt(total));
								if (String.valueOf(rs.getString(msisdn))!=null && nvl > 0){
									//do decrement
									upd="update atp_max_redeem set total = nvl(total,0) - 1 where msisdn = " + String.valueOf(rs.getString(msisdn)) + " and evt_id = " + PROGRAM_ID_ATP_KEYWORDS + " and period = " + vperiod;
									stmt = conn.createStatement();
									stmt.executeUpdate(upd);
								}
							}

							if (null != rs)
								rs.close();
							if (null != conn)
								conn.close();
							if (null != stmt)
								stmt.close();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (SQLException e) {
							e.printStackTrace();
						} finally {
							try {
								if (null != rs)
									rs.close();
								if (null != conn)
									conn.close();
								if (null != stmt)
									stmt.close();
							} catch (SQLException e1) {
								e1.printStackTrace();
							}
						}

    }
					
    	PERR_CODE= "000";
    	PERR_MSG = "Success";
    	
    	
		listParameter.add(new ParameterKey(Constant.APMN_MSG_CODE,"SUCCESS_OSB_REFUND_POIN"));
		String resultAk = cache.getSingleDataHashMap(listParameter,"ATP_PARAMETER_MSG_NON");

		if (null != resultAk) {
			String arryResultAk[] = resultAk.split("\\|");
			RESP_CODE_ATP_PARAMETER_MSG_NON= arryResultAk[Constant.VAPMN_RESP_CODE];
			P_NOTIF= arryResultAk[Constant.VAPMN_MSG_TEXT];
			MSG_CODE_ATP_PARAMETER_MSG_NON= arryResultAk[Constant.VAPMN_MSG_CODE];
			MSG_TYPE_ATP_PARAMETER_MSG_NON= arryResultAk[Constant.VAPMN_MSG_TYPE];
			MSG_SEQ_ATP_PARAMETER_MSG_NON= arryResultAk[Constant.VAPMN_MSG_SEQ];
		}
    	
		
		PRC_CP_POSTPAID_REFUND.set_PERR_CODE(PERR_CODE);
		PRC_CP_POSTPAID_REFUND.set_PERR_MSG(PERR_MSG);
		PRC_CP_POSTPAID_REFUND.set_XP_MSISDN(XP_MSISDN);
		PRC_CP_POSTPAID_REFUND.set_P_NOTIF(P_NOTIF);

		return PRC_CP_POSTPAID_REFUND;
		
		
			  }
		 conn.commit();// <-- commit insert di class PRC_REFUND_POIN
		 // dicommit <-- commit insert di class PRC_REFUND_POIN
		 
	}
}

