

class DetectAndFixOutliers:
    def __init__(self,detection_folder_path,detection_res_df_save_file_path,fixed_folder_path,apd_data_for_fix_folder_path):
        self.detection_folder_path = detection_folder_path
        self.detection_res_df_save_file_path = detection_res_df_save_file_path
        self.fixed_folder_path = fixed_folder_path
        self.apd_data_for_fix_folder_path = apd_data_for_fix_folder_path
        
    
    def detect_outliers_data(self,detect_method:function):
        """
        检测异常值
        :param detect_method: function
        :return: outlier_df: pd.DataFrame
        """
        pass
    
    def detect_outliers_data_from_detection_folder_path(self):
        """
        从检测文件夹中检测异常值
        :return: outliers_dict: dict, key: file_name, value: pd.DataFrame
        """
        pass

    def filter_detected_outliers(self,filter_method:function):
        """
        过滤检测到的异常值
        :param filter_method: function
        :return: filtered_df: pd.DataFrame
        """
        pass
        
        
    def fix_outliers_data(self,fix_method:function):
        """
        修正异常值
        :param fix_method: function
        :return: fixed_df: pd.DataFrame
        """
        pass
    
    def fix_filtered_outliers_from_fixed_folder_path(self):
        """
        从修正文件夹中修正异常值
        :return: fixed_dict: dict, key: file_name, value: pd.DataFrame
        """
        pass
    
    