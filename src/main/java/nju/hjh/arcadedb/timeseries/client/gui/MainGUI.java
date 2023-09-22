package nju.hjh.arcadedb.timeseries.client.gui;

import com.alibaba.fastjson2.JSONObject;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.stage.Stage;
import nju.hjh.arcadedb.timeseries.client.ArcadeTSDBClient;

public class MainGUI extends Application {
    private ArcadeTSDBClient client = new ArcadeTSDBClient();
    private MainControl mainControl;
    public Scene mainScene;

    @Override
    public void init() throws Exception {
        super.init();
        client.connect();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            client.close();
        }));
    }

    @Override
    public void start(Stage stage) throws Exception {
        if (!client.isConnected()){
            Alert alert = new Alert(Alert.AlertType.ERROR, String.format("未能连接至服务器%s\n请检查网络设置", client.getHost()));
            alert.setTitle("服务器连接失败");
            alert.show();
            return;
        }

        FXMLLoader fxmlLoader = new FXMLLoader(this.getClass().getResource("main_view.fxml"));
        mainScene = new Scene(fxmlLoader.load());
        mainControl = fxmlLoader.getController();
        mainControl.init(this);

        // quick send message using Alt+Enter
        KeyCombination CtrlEnter = new KeyCodeCombination(KeyCode.ENTER, KeyCombination.ALT_DOWN);
        mainScene.getAccelerators().put(CtrlEnter, new Runnable() {
            @Override
            public void run() {
                mainControl.btnSend.fire();
            }
        });

        stage.setTitle("服务器："+client.getHost());
        stage.setScene(mainScene);
        stage.show();

        stage.setMinHeight(stage.getHeight());
        stage.setMinWidth(stage.getWidth());
    }

    public String sendMsgAndWaitResult(JSONObject msgObject){
        return client.sendJsonAndWaitResult(msgObject);
    }

    public static void main(String[] args) {
        launch();
    }
}
