package nju.hjh.arcadedb.timeseries.client.gui;

import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;
import javafx.event.ActionEvent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import nju.hjh.arcadedb.timeseries.client.ClientUtils;
import nju.hjh.utils.exception.ExceptionSerializer;

public class MainControl {
    public TextArea txtReceive;
    public TextArea txtSend;
    public Button btnSend;
    public Label txtHistoryIndex;
    private MainGUI mainGUI;

    public void init(MainGUI mainGUI){
        this.mainGUI = mainGUI;
        txtHistoryIndex.setText("NEW");
    }

    private int getLineIndex(String content, int caretPosition){
        int curLine = 0;
        String beforeCaret = content.substring(0, caretPosition);
        int curIndex = beforeCaret.indexOf('\n');
        while (curIndex != -1){
            curLine++;
            curIndex = beforeCaret.indexOf('\n', curIndex+1);
        }
        return curLine;
    }

    private int getLineEndCaretPos(String content, int lineIndex){
        int caretPos = -1;
        for (int i=0; i<=lineIndex; i++){
            int nextPos = content.indexOf('\n', caretPos+1);
            if (nextPos == -1){
                caretPos = content.length();
                break;
            }
            caretPos = nextPos;
        }
        return caretPos;
    }

    private String formatSendText(String newText){
        int lineIndex = getLineIndex(txtSend.getText(), txtSend.getCaretPosition());
        String msgFormat = ClientUtils.toPrettyPrintJSON(newText);

        txtSend.setText(msgFormat);
        txtSend.positionCaret(getLineEndCaretPos(msgFormat, lineIndex));
        return msgFormat;
    }

    public void btnSend_clicked(ActionEvent actionEvent) {
        JSONObject jsonMsg;
        try {
            jsonMsg = JSONObject.parseObject(txtSend.getText());
        }catch (JSONException e){
            txtReceive.setText(ExceptionSerializer.serializeAll(e));
            mainGUI.recordHistory(formatSendText(txtSend.getText()));
            return;
        }
        mainGUI.recordHistory(formatSendText(jsonMsg.toJSONString()));

        long begin = System.currentTimeMillis();
        String msgRet = mainGUI.sendMsgAndWaitResult(jsonMsg);
        long elapsed = System.currentTimeMillis() - begin;
        if (msgRet == null)
            txtReceive.setText("服务器无响应");
        else
            txtReceive.setText(String.format("用时%dms，服务器返回：\n%s", elapsed, ClientUtils.toPrettyPrintJSON(msgRet)));
    }
}
