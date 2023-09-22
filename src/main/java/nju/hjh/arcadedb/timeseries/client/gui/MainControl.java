package nju.hjh.arcadedb.timeseries.client.gui;

import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;
import javafx.event.ActionEvent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import nju.hjh.arcadedb.timeseries.client.ClientUtils;
import nju.hjh.arcadedb.timeseries.server.ServerUtils;
import nju.hjh.utils.exception.ExceptionSerializer;

public class MainControl {
    public TextArea txtReceive;
    public TextArea txtSend;
    public Button btnSend;
    private MainGUI mainGUI;

    public void init(MainGUI mainGUI){
        this.mainGUI = mainGUI;
    }

    public void btnSend_clicked(ActionEvent actionEvent) {
        JSONObject jsonMsg;
        try {
            jsonMsg = JSONObject.parseObject(txtSend.getText());
        }catch (JSONException e){
            txtReceive.setText(ExceptionSerializer.serializeAll(e));
            return;
        }

        // format user input
        String beforeCaret = txtSend.getText(0, txtSend.getCaretPosition());
        int curLine = 0;
        int curIndex = beforeCaret.indexOf('\n');
        while (curIndex != -1){
            curLine++;
            curIndex = beforeCaret.indexOf('\n', curIndex+1);
        }
        String prettyJson = ClientUtils.toPrettyPrintJSON(jsonMsg.toJSONString());
        int newCaretPos = -1;
        for (int i=0; i<=curLine; i++){
            int nextPos = prettyJson.indexOf('\n', newCaretPos+1);
            if (nextPos == -1){
                newCaretPos = prettyJson.length();
                break;
            }
            newCaretPos = nextPos;
        }
        txtSend.setText(prettyJson);
        txtSend.positionCaret(newCaretPos);

        long begin = System.currentTimeMillis();
        String msgRet = mainGUI.sendMsgAndWaitResult(jsonMsg);
        long elapsed = System.currentTimeMillis() - begin;
        if (msgRet == null)
            txtReceive.setText("服务器无响应");
        else
            txtReceive.setText(String.format("用时%dms，服务器返回：\n%s", elapsed, ClientUtils.toPrettyPrintJSON(msgRet)));
    }
}
