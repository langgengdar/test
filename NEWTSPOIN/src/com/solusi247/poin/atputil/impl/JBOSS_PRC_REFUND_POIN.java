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

public class JBOSS_PRC_REFUND_POIN {
	static private Logger logger = logger.getL.getLogger(JBOSS_PRC_REFUND_POIN.class);
	private ManagementDataCache cache = new ManagementDataCache();
	private Helper helper = new Helper();
	String PTRX_ID;// IN VARCHAR2,
	String PCHANNEL;// IN VARCHAR2,
	String IN_MSISDN;// IN VARCHAR2,
	String PERR_CODE;// OUT VARCHAR2,
	String PERR_MSG;// OUT VARCHAR2,
	String XP_MSISDN;// OUT VARCHAR2,
	String XP_KEYWORD;// OUT VARCHAR2,
	String XP_PERIOD;// OUT VARCHAR2
	

	public PRC_REFUND_POIN execute(Jboss_Atp_Util atp_util) {
		String sql;
		String D_TRREDEEM_TRRDM_MSISDN;
		String D_TRREDEEM_TRRDM_KEYWORD;
		String D_TRREDEEM_TRRDM_EXEC_DATE;
		String D_TRREDEEM_TRRDM_SBR_ID;
		String CR_EVT_SET_ID;
		String D_TRREDEEM_TRRDM_EVD_ID;
		int S_TKN_NEXTVAL;
		Statement stmt = null;
		ResultSet rs = null;
		String x_up_msisdn;
		String x_wallet_id;
		String x_poin;

		String cd_date_eff;
		String cd_date_exp;
		String cd_usg_point;
		
		PreparedStatement ps = null;
		Connection conn = null;
		
		SimpleDateFormat format1 = new SimpleDateFormat("YYYYMMDD");
		String dbURL = "jdbc:oracle:thin:@10.2.232.93:1521:ophpoint";
		String username = "newtspoin";
		String password = "newtspoin";
		sql = "SELECT S_TKN.NEXTVAL NEXTVAL FROM DUAL";

		try {
				Class.forName("oracle.jdbc.OracleDriver");
				conn = DriverManager.getConnection(dbURL, username,password);
				conn.setAutoCommit(false);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					S_TKN_NEXTVAL = String.valueOf(rs.getInt(NEXTVAL));
				}

				if (null != rs)
					rs.close();
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
				if (null != stmt)
					stmt.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}


		String VCMD;//	     VARCHAR2(2000);
		int VLENGTH=0;//    NUMBER:=0;
		String VSQLERR;//    VARCHAR2(200);
		int VSQLCODE;//   NUMBER(10);
		int vcek=0;//       SMALLINT := 0;
		int vcpts=0;//    INTEGER := 0;
		int v_id;//     NUMBER;
		String VURL=null;//      LOG_API.URL_ADDRESS%TYPE := NULL;
		String VRETVAL=null;//   VARCHAR2(1000) := NULL;
		String VREQ=null;//      LOG_API.REQUEST_METHOD%TYPE := NULL;
		String VLOGID;//       VARCHAR2(1024);
		String P_API_NAME= "";
		String VTRXID= "";
		String VHTPP= "";
		String VKEYWORD;
		String vperiod;

		VTRXID= PTRX_ID;
		VKEYWORD= "";


		sql = "select * from ATP_TR_REDEEM where trrdm_msisdn = " + IN_MSISDN + " and trrdm_tkn_id = " + PTRX_ID + " and trrdm_status <> 8";
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				D_TRREDEEM_TRRDM_MSISDN= String.valueOf(rs.getString(TRRDM_MSISDN));
				D_TRREDEEM_TRRDM_KEYWORD= String.valueOf(rs.getString(TRRDM_KEYWORD));
				D_TRREDEEM_TRRDM_EXEC_DATE= String.valueOf(rs.getString(TRRDM_EXEC_DATE));
				D_TRREDEEM_TRRDM_SBR_ID= String.valueOf(rs.getString(TRRDM_SBR_ID));
				D_TRREDEEM_TRRDM_EVD_ID= String.valueOf(rs.getString(TRRDM_EVD_ID));
			}

			if (null != rs)
				rs.close();
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
				if (null != stmt)
					stmt.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		Calendar calendar = Calendar.getInstance();
		XP_PERIOD=format1.format(calendar.getTime());//helper.dateToString("YYYYMMDD", new Date());
	  
		if (D_TRREDEEM_TRRDM_MSISDN==null){
			PERR_CODE= "006";
			PERR_MSG= "Redeem transaction is not found";
		}else{
			VKEYWORD= D_TRREDEEM_TRRDM_KEYWORD;
	        XP_KEYWORD= D_TRREDEEM_TRRDM_KEYWORD;
	        XP_MSISDN= D_TRREDEEM_TRRDM_MSISDN;
	        vperiod= format1.format(D_TRREDEEM_TRRDM_EXEC_DATE);
	        // vperiod= TO_CHAR(D_TRREDEEM_trrdm_exec_date, 'YYYYMMDD');
	        XP_PERIOD= vperiod;
	        JBOSS_atp_util JBOSS_atp_util=new JBOSS_atp_util;
	        jboss_atp_util.reset_variable(D_TRREDEEM_TRRDM_MSISDN,VKEYWORD,PTRX_ID);
	        atp_util.vtkn_id= PTRX_ID;
	        logger.info("atp_util.vevt_group_id: " + atp_util.vevt_group_id);
	        
	        sql = "SELECT a.program_id,  a.program_name, b.sms_type, b.keyword_id, c.pointin_code ,c.evt_set_id FROM atp_program_header  a,  atp_keywords  b,  atp_parameter_pointin  c ,  atp_event_sets  d WHERE  a.program_id = b.evt_id_reff AND b.evt_id_reff= c.evt_id_reff AND b.keyword_id = b.keyword_id AND c.evt_set_id = d.evt_set_id AND b.sms_type = 'INJECT' AND b.evt_group_id =  " + atp_util.vevt_group_id + " AND a.preff_id = '10'";

			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					CR_EVT_SET_ID= String.valueOf(rs.getString(EVT_SET_ID));
				}

				if (null != rs)
					rs.close();
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
					if (null != stmt)
						stmt.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		

	        
			logger.info("cr.evt_set_id: " + CR_EVT_SET_ID);
			sql = "SELECT seq_atp_ids.NEXTVAL NEXTVAL FROM dual";

			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					v_id = String.valueOf(rs.getInt(NEXTVAL));
				}

				if (null != rs)
					rs.close();
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
					if (null != stmt)
						stmt.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
					
	        
	        
			vcpts = 0;
			sql = "SELECT  ABS(up_point) poin,up_date_eff,up_date_exp,up_evd_set_id, up_msisdn, Substr(up_msg, instr(up_msg,' ',-1)+1, length(up_msg)) wallet_id FROM atp_uposted_points WHERE up_msisdn = " + D_TRREDEEM_TRRDM_MSISDN + " AND up_evd_id = " + D_TRREDEEM_trrdm_evd_id;
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					x_up_msisdn= String.valueOf(rs.getString(up_msisdn));
					x_wallet_id= String.valueOf(rs.getString(wallet_id));
					x_poin= String.valueOf(rs.getInt(poin));
		 			sql =  "SELECT * FROM atp_wallet_point WHERE msisdn = " + x_up_msisdn + " AND wallet_point_id = " + x_wallet_id;
					try {
						stmt = conn.createStatement();
						rs1 = stmt.executeQuery(sql);
						while (rs.next()) {
							cd_date_eff=String.valueOf(rs.getString(date_eff));
							cd_date_exp=String.valueOf(rs.getString(date_exp));
							cd_usg_point=String.valueOf(rs.getString(usg_point));
						}

						if (null != rs)
							rs.close();
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
							if (null != stmt)
								stmt.close();
						} catch (SQLException e1) {
							e1.printStackTrace();
						}
					}
	                
					logger.info("inloop cd_usg_point: " + cd_usg_point);
	                
	                vcpts= vcpts +  x_poin;
	                ///--------------------------
	                String Insert_sql_atp_uposted_points = "INSERT INTO atp_uposted_points(up_sbr_id,up_msisdn,up_name,up_evd_id,up_evd_set_id,up_evd_period,up_evd_value,up_point,up_nik,up_load_id,up_err_msg,up_msg,up_posted,up_date_eff, up_date_exp)VALUES (" + D_TRREDEEM_trrdm_sbr_id + ", " + D_TRREDEEM_trrdm_msisdn + ", null, " + D_TRREDEEM_trrdm_evd_id + ", " + CR_EVT_SET_ID + ", SYSDATE ,abs(" + x_poin + "), abs(" + x_poin + "), " + PCHANNEL,v_id + ",'.','Refund '" + VTRXID + ",1, " + cd_date_eff + ", " + cd_date_exp + ")";
	                try {
	                	stmt.execute(Insert_sql_atp_uposted_points);
	                } catch (ClassNotFoundException e) {
						e.printStackTrace();
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						try {
							if (null != stmt)
								stmt.close();
						} catch (SQLException e1) {
							e1.printStackTrace();
						}
					}
	                 String insert_atp_Wallet_point="insert into atp_Wallet_point (WALLET_POINT_ID,SBR_ID,MSISDN,EVT_SET_ID,PRIORITY,PREFIX,NUM_POINT,DATE_GIVEN,DATE_EFF,DATE_EXP,LAST_POINT,USG_POINT,YEARMONTH)VALUES (SEQ_WALLET.NEXTVAL, " + D_TRREDEEM_trrdm_sbr_id + ", " + D_TRREDEEM_trrdm_msisdn + ", " + CR_EVT_SET_ID + ", 1, substr(" + D_TRREDEEM_trrdm_msisdn + ",1,4),abs(" + x_poin + "),SYSDATE, " + cd_date_eff + ", " + cd_date_exp + ", abs(" + x_poin + ") ,0,to_char(" + cd_date_exp + ",'rrrrmm'))";
	                 try {
	                	 stmt.execute(insert_atp_Wallet_point);
				    } catch (ClassNotFoundException e) {
						e.printStackTrace();
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						try {
							if (null != stmt)
								stmt.close();
						} catch (SQLException e1) {
							e1.printStackTrace();
						}
	                            //masukanke wallets
					}
				}

	                 if (null != rs)
	                	 rs.close();
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
							if (null != stmt)
								stmt.close();
						} catch (SQLException e1) {
							e1.printStackTrace();
						}
					}

	            
	          //update transaction status with 8
	         String update_atp_tr_redeem="UPDATE atp_tr_redeem SET trrdm_exec_by = " + PCHANNEL + ",trrdm_status    = '8' WHERE trrdm_evd_id = " + D_TRREDEEM_trrdm_evd_id;
	         try {
	        	 stmt.executeUpdate(update_atp_tr_redeem);
	         } catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					try {
						if (null != stmt)
							stmt.close();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
	        PERR_CODE= "000";
	        PERR_MSG= "Success";

	
	  }
		
		
	}
}
