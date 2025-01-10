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

import java.util.ArrayList;

public class MainGUI extends Application {
    private ArcadeTSDBClient client = new ArcadeTSDBClient();
    private MainControl mainControl;
    public Scene mainScene;
    public ArrayList<String> history;
    public int curHistoryIndex;

    @Override
    public void init() throws Exception {
        super.init();
        client.connect();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            client.close();
        }));

        history = new ArrayList<>();
        curHistoryIndex = -1;
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

        // quick send message
        mainScene.getAccelerators().put(new KeyCodeCombination(KeyCode.ENTER, KeyCombination.ALT_DOWN), new Runnable() {
            @Override
            public void run() {
                mainControl.btnSend.fire();
            }
        });

        // hsitory access
        mainScene.getAccelerators().put(new KeyCodeCombination(KeyCode.LEFT, KeyCombination.ALT_DOWN), new Runnable() {
            @Override
            public void run() {
                loadPrevHistory();
            }
        });
        mainScene.getAccelerators().put(new KeyCodeCombination(KeyCode.RIGHT, KeyCombination.ALT_DOWN), new Runnable() {
            @Override
            public void run() {
                loadNextHistory();
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

    public void loadPrevHistory(){
        if (curHistoryIndex <= 0) return;
        curHistoryIndex--;
        mainControl.txtSend.setText(history.get(curHistoryIndex));
        mainControl.txtHistoryIndex.setText(String.valueOf(curHistoryIndex));
    }

    public void loadNextHistory(){
        if (curHistoryIndex >= history.size()-1) return;
        curHistoryIndex++;
        mainControl.txtSend.setText(history.get(curHistoryIndex));
        mainControl.txtHistoryIndex.setText(String.valueOf(curHistoryIndex));
    }

    public void recordHistory(String message){
        history.add(message);
        curHistoryIndex = history.size()-1;
        mainControl.txtHistoryIndex.setText(String.valueOf(curHistoryIndex));
    }

    public static void main(String[] args) {
        launch();
    }
}
